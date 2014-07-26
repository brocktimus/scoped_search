module ScopedSearch
  require 'scoped_search/query_builder/field'
  require 'scoped_search/query_builder/leaf_node'
  require 'scoped_search/query_builder/operator_node'
  require 'scoped_search/query_builder/logical_operator_node'

  # The QueryBuilder class builds an SQL query based on aquery string that is
  # provided to the search_for named scope. It uses a SearchDefinition instance
  # to shape the query.
  class QueryBuilder

    attr_reader :ast, :definition

    # Creates a find parameter hash that can be passed to ActiveRecord::Base#find,
    # given a search definition and query string. This method is called from the
    # search_for named scope.
    #
    # This method will parse the query string and build an SQL query using the search
    # query. It will return an empty hash if the search query is empty, in which case
    # the scope call will simply return all records.
    def self.build_query(definition, *args)
      query = args[0] ||=''
      options = args[1] || {}

      query_builder_class = self.class_for(definition)
      if query.kind_of?(ScopedSearch::QueryLanguage::AST::Node)
        return query_builder_class.new(definition, query, options[:profile]).build_find_params(options)
      elsif query.kind_of?(String)
        return query_builder_class.new(definition, ScopedSearch::QueryLanguage::Compiler.parse(query), options[:profile]).build_find_params(options)
      else
        raise "Unsupported query object: #{query.inspect}!"
      end
    end

    # Loads the QueryBuilder class for the connection of the given definition.
    # If no specific adapter is found, the default QueryBuilder class is returned.
    def self.class_for(definition)
      self.const_get(definition.klass.connection.class.name.split('::').last)
    rescue
      self
    end

    # Initializes the instance by setting the relevant parameters
    def initialize(definition, ast, profile)
      @definition, @ast, @definition.profile = definition, ast, profile
    end

    # Actually builds the find parameters hash that should be used in the search_for
    # named scope.
    def build_find_params(options)
      keyconditions = []
      keyparameters = []
      parameters = []
      includes   = []
      joins   = []

      # Build SQL WHERE clause using the AST
      sql = @ast.to_sql(self, definition) do |notification, value|

        # Handle the notifications encountered during the SQL generation:
        # Store the parameters, includes, etc so that they can be added to
        # the find-hash later on.
        case notification
          when :keycondition then keyconditions << value
          when :keyparameter then keyparameters << value
          when :parameter    then parameters    << value
          when :include      then includes      << value
          when :joins        then joins         << value
          else raise ScopedSearch::QueryNotSupported, "Cannot handle #{notification.inspect}: #{value.inspect}"
        end
      end
        # Build SQL ORDER BY clause
      order = order_by(options[:order]) do |notification, value|
        case notification
          when :parameter then parameters << value
          when :include   then includes   << value
          when :joins     then joins      << value
          else raise ScopedSearch::QueryNotSupported, "Cannot handle #{notification.inspect}: #{value.inspect}"
        end
      end
      sql = (keyconditions + (sql.blank? ? [] : [sql]) ).map {|c| "(#{c})"}.join(" AND ")
      # Build hash for ActiveRecord::Base#find for the named scope
      find_attributes = {}
      find_attributes[:conditions] = [sql] + keyparameters + parameters unless sql.blank?
      find_attributes[:include]    = includes.uniq                      unless includes.empty?
      find_attributes[:joins]      = joins.uniq                         unless joins.empty?
      find_attributes[:order]      = order                              unless order.nil?

      # p find_attributes # Uncomment for debugging
      return find_attributes
    end

    def order_by(order, &block)
      order ||= definition.default_order
      return nil if order.blank?
      field_name, direction_name = order.to_s.split(/\s+/, 2)
      field = definition.field_by_name(field_name)
      raise ScopedSearch::QueryNotSupported, "the field '#{field_name}' in the order statement is not valid field for search" unless field
      sql = field.to_sql(&block)
      direction = (!direction_name.nil? && direction_name.downcase.eql?('desc')) ? " DESC" : " ASC"
      order = sql + direction

      return order
    end

    # A hash that maps the operators of the query language with the corresponding SQL operator.
    SQL_OPERATORS = { :eq => '=',  :ne => '<>', :like => 'LIKE', :unlike => 'NOT LIKE',
                      :gt => '>',  :lt =>'<',   :lte => '<=',    :gte => '>=',
                      :in => 'IN', :notin => 'NOT IN' }

    # Return the SQL operator to use given an operator symbol and field definition.
    #
    # By default, it will simply look up the correct SQL operator in the SQL_OPERATORS
    # hash, but this can be overridden by a database adapter.
    def sql_operator(operator, field)
      raise ScopedSearch::QueryNotSupported, "the operator '#{operator}' is not supported for field type '#{field.type}'" if [:like, :unlike].include?(operator) and !field.textual?
      SQL_OPERATORS[operator]
    end

    # Returns a NOT (...)  SQL fragment that negates the current AST node's children
    def to_not_sql(rhs, definition, &block)
      "NOT COALESCE(#{rhs.to_sql(self, definition, &block)}, 0)"
    end

    # Perform a comparison between a field and a Date(Time) value.
    #
    # This function makes sure the date is valid and adjust the comparison in
    # some cases to return more logical results.
    #
    # This function needs a block that can be used to pass other information about the query
    # (parameters that should be escaped, includes) to the query builder.
    #
    # <tt>field</tt>:: The field to test.
    # <tt>operator</tt>:: The operator used for comparison.
    # <tt>value</tt>:: The value to compare the field with.
    def datetime_test(field, operator, value, &block) # :yields: finder_option_type, value

      # Parse the value as a date/time and ignore invalid timestamps
      timestamp = definition.parse_temporal(value)
      return nil unless timestamp

      timestamp = timestamp.to_date if field.date?
      # Check for the case that a date-only value is given as search keyword,
      # but the field is of datetime type. Change the comparison to return
      # more logical results.
      if field.datetime?
        span = 1.minute if(value =~ /\A\s*\d+\s+\bminutes?\b\s+\bago\b\s*\z/i)
        span ||= (timestamp.day_fraction == 0) ? 1.day : 1.hour
        if [:eq, :ne].include?(operator)
          # Instead of looking for an exact (non-)match, look for dates that
          # fall inside/outside the range of timestamps of that day.
          yield(:parameter, timestamp)
          yield(:parameter, timestamp + span)
          negate    = (operator == :ne) ? 'NOT ' : ''
          field_sql = field.to_sql(operator, &block)
          return "#{negate}(#{field_sql} >= ? AND #{field_sql} < ?)"

        elsif operator == :gt
          # Make sure timestamps on the given date are not included in the results
          # by moving the date to the next day.
          timestamp += span
          operator = :gte

        elsif operator == :lte
          # Make sure the timestamps of the given date are included by moving the
          # date to the next date.
          timestamp += span
          operator = :lt
        end
      end

      # Yield the timestamp and return the SQL test
      yield(:parameter, timestamp)
      "#{field.to_sql(operator, &block)} #{sql_operator(operator, field)} ?"
    end

    # Validate the key name is in the set and translate the value to the set value.
    def translate_value(field, value)
      translated_value = field.complete_value[value.to_sym]
      raise ScopedSearch::QueryNotSupported, "'#{field.field}' should be one of '#{field.complete_value.keys.join(', ')}', but the query was '#{value}'" if translated_value.nil?
      translated_value
    end

    # A 'set' is group of possible values, for example a status might be "on", "off" or "unknown" and the database representation
    # could be for example a numeric value. This method will validate the input and translate it into the database representation.
    def set_test(field, operator,value, &block)
      set_value = translate_value(field, value)
      raise ScopedSearch::QueryNotSupported, "Operator '#{operator}' not supported for '#{field.field}'" unless [:eq,:ne].include?(operator)
      negate = ''
      if [true,false].include?(set_value)
        negate = 'NOT ' if operator == :ne
        if field.numerical?
          operator =  (set_value == true) ?  :gt : :eq
          set_value = 0
        else
          operator = (set_value == true) ? :ne : :eq
          set_value = false
        end
      end
      yield(:parameter, set_value)
      return "#{negate}(#{field.to_sql(operator, &block)} #{self.sql_operator(operator, field)} ?)"
    end

    # Generates a simple SQL test expression, for a field and value using an operator.
    #
    # This function needs a block that can be used to pass other information about the query
    # (parameters that should be escaped, includes) to the query builder.
    #
    # <tt>field</tt>:: The field to test.
    # <tt>operator</tt>:: The operator used for comparison.
    # <tt>value</tt>:: The value to compare the field with.
    def sql_test(field, operator, value, lhs, &block) # :yields: finder_option_type, value
      return field.to_ext_method_sql(lhs, sql_operator(operator, field), value, &block) if field.ext_method

      yield(:keyparameter, lhs.sub(/^.*\./,'')) if field.key_field

      if [:like, :unlike].include?(operator)
        yield(:parameter, (value !~ /^\%|\*/ && value !~ /\%|\*$/) ? "%#{value}%" : value.tr_s('%*', '%'))
        return "#{field.to_sql(operator, &block)} #{self.sql_operator(operator, field)} ?"
      elsif [:in, :notin].include?(operator)
        value.split(',').collect { |v| yield(:parameter, field.set? ? translate_value(field, v) : v.strip) }
        value = value.split(',').collect { "?" }.join(",")
        return "#{field.to_sql(operator, &block)} #{self.sql_operator(operator, field)} (#{value})"
      elsif field.temporal?
        return datetime_test(field, operator, value, &block)
      elsif field.set?
        return set_test(field, operator, value, &block)
      elsif field.definition.klass.reflections[field.relation].try(:macro) == :has_many
        value = value.to_i if field.offset
        yield(:parameter, value)
        connection = field.definition.klass.connection
        primary_key = "#{connection.quote_table_name(field.definition.klass.table_name)}.#{connection.quote_column_name(field.definition.klass.primary_key)}"
        if field.definition.klass.reflections[field.relation].options.has_key?(:through)
          join = has_many_through_join(field)
          return "#{primary_key} IN (SELECT #{primary_key} FROM #{join} WHERE #{field.to_sql(operator, &block)} #{self.sql_operator(operator, field)} ? )"
        else
          foreign_key = connection.quote_column_name(field.reflection_keys(field.definition.klass.reflections[field.relation])[1])
          return "#{primary_key} IN (SELECT #{foreign_key} FROM #{connection.quote_table_name(field.klass.table_name)} WHERE #{field.to_sql(operator, &block)} #{self.sql_operator(operator, field)} ? )"
        end
      else
        value = value.to_i if field.offset
        yield(:parameter, value)
        return "#{field.to_sql(operator, &block)} #{self.sql_operator(operator, field)} ?"
      end
    end

    def has_many_through_join(field)
      many_class = field.definition.klass
      through = many_class.reflections[field.relation].options[:through]
      connection = many_class.connection

      # table names
      endpoint_table_name = field.klass.table_name
      many_table_name = many_class.table_name
      middle_table_name = many_class.reflections[through].klass.table_name

      # primary and foreign keys + optional condition for the many to middle join
      pk1, fk1   = field.reflection_keys(many_class.reflections[through])
      condition1 = field.reflection_conditions(field.klass.reflections[many_table_name.to_sym])

      # primary and foreign keys + optional condition for the endpoint to middle join
      pk2, fk2   = field.reflection_keys(field.klass.reflections[middle_table_name.to_sym])
      condition2 = field.reflection_conditions(many_class.reflections[field.relation])

      <<-SQL
        #{connection.quote_table_name(many_table_name)}
        INNER JOIN #{connection.quote_table_name(middle_table_name)}
        ON #{connection.quote_table_name(many_table_name)}.#{connection.quote_column_name(pk1)} = #{connection.quote_table_name(middle_table_name)}.#{connection.quote_column_name(fk1)} #{condition1}
        INNER JOIN #{connection.quote_table_name(endpoint_table_name)}
        ON #{connection.quote_table_name(middle_table_name)}.#{connection.quote_column_name(fk2)} = #{connection.quote_table_name(endpoint_table_name)}.#{connection.quote_column_name(pk2)} #{condition2}
      SQL
    end
  end

  # The MysqlAdapter makes sure that case sensitive comparisons are used
  # when using the (not) equals operator, regardless of the field's
  # collation setting.
  class MysqlAdapter < ScopedSearch::QueryBuilder

    # Patches the default <tt>sql_operator</tt> method to add
    # <tt>BINARY</tt> after the equals and not equals operator to force
    # case-sensitive comparisons.
    def sql_operator(operator, field)
      if [:ne, :eq].include?(operator) && field.textual?
        "#{SQL_OPERATORS[operator]} BINARY"
      else
        super(operator, field)
      end
    end
  end

  class Mysql2Adapter < ScopedSearch::QueryBuilder
     # Patches the default <tt>sql_operator</tt> method to add
    # <tt>BINARY</tt> after the equals and not equals operator to force
    # case-sensitive comparisons.
    def sql_operator(operator, field)
      if [:ne, :eq].include?(operator) && field.textual?
        "#{SQL_OPERATORS[operator]} BINARY"
      else
        super(operator, field)
      end
    end
  end

  # The PostgreSQLAdapter make sure that searches are case sensitive when
  # using the like/unlike operators, by using the PostrgeSQL-specific
  # <tt>ILIKE operator</tt> instead of <tt>LIKE</tt>.
  class PostgreSQLAdapter < ScopedSearch::QueryBuilder

    # Switches out the default query generation of the <tt>sql_test</tt>
    # method if full text searching is enabled and a text search is being
    # performed.
    def sql_test(field, operator, value, lhs, &block)
      if [:like, :unlike].include?(operator) and field.full_text_search
        yield(:parameter, value)
        negation = (operator == :unlike) ? "NOT " : ""
        locale = (field.full_text_search == true) ? 'english' : field.full_text_search
        return "#{negation}to_tsvector('#{locale}', #{field.to_sql(operator, &block)}) #{self.sql_operator(operator, field)} to_tsquery('#{locale}', ?)"
      else
        super
      end
    end

    # Switches out the default LIKE operator in the default <tt>sql_operator</tt> 
    # method for ILIKE or @@ if full text searching is enabled.
    def sql_operator(operator, field)
      raise ScopedSearch::QueryNotSupported, "the operator '#{operator}' is not supported for field type '#{field.type}'" if [:like, :unlike].include?(operator) and !field.textual?
      return '@@' if [:like, :unlike].include?(operator) and field.full_text_search
      case operator
        when :like   then 'ILIKE'
        when :unlike then 'NOT ILIKE'
        else super(operator, field)
      end
    end

    # Returns a NOT (...)  SQL fragment that negates the current AST node's children
    def to_not_sql(rhs, definition, &block)
      "NOT COALESCE(#{rhs.to_sql(self, definition, &block)}, false)"
    end

    def order_by(order, &block)
      sql = super(order, &block)
      sql += sql.include?('DESC') ? ' NULLS LAST ' : ' NULLS FIRST ' if sql
      sql
    end
  end

  # The Oracle adapter also requires some tweaks to make the case insensitive LIKE work.
  class OracleEnhancedAdapter < ScopedSearch::QueryBuilder

    def sql_test(field, operator, value, lhs, &block) # :yields: finder_option_type, value
      if field.key_field
        yield(:parameter, lhs.sub(/^.*\./,''))
      end
      if field.textual? && [:like, :unlike].include?(operator)
        yield(:parameter, (value !~ /^\%|\*/ && value !~ /\%|\*$/) ? "%#{value}%" : value.to_s.tr_s('%*', '%'))
        return "LOWER(#{field.to_sql(operator, &block)}) #{self.sql_operator(operator, field)} LOWER(?)"
      elsif field.temporal?
        return datetime_test(field, operator, value, &block)
      else
        yield(:parameter, value)
        return "#{field.to_sql(operator, &block)} #{self.sql_operator(operator, field)} ?"
      end
    end
  end
end
