class ScopedSearch::QueryBuilder
  # This module gets included into the Field class to add SQL generation.
  module Field

    # Return an SQL representation for this field. Also make sure that
    # the relation which includes the search field is included in the
    # SQL query.
    #
    # This function may yield an :include that should be used in the
    # ActiveRecord::Base#find call, to make sure that the field is available
    # for the SQL query.
    def to_sql(operator = nil, &block) # :yields: finder_option_type, value
      num = rand(1000000)
      connection = klass.connection
      if key_relation
        yield(:joins, construct_join_sql(key_relation, num) )
        yield(:keycondition, "#{key_klass.table_name}_#{num}.#{connection.quote_column_name(key_field.to_s)} = ?")
        klass_table_name = relation ? "#{klass.table_name}_#{num}" : klass.table_name
        return "#{connection.quote_table_name(klass_table_name)}.#{connection.quote_column_name(field.to_s)}"
      elsif key_field
        yield(:joins, construct_simple_join_sql(num))
        yield(:keycondition, "#{key_klass.table_name}_#{num}.#{connection.quote_column_name(key_field.to_s)} = ?")
        klass_table_name = relation ? "#{klass.table_name}_#{num}" : klass.table_name
        return "#{connection.quote_table_name(klass_table_name)}.#{connection.quote_column_name(field.to_s)}"
      elsif relation
        yield(:include, relation)
      end
      column_name = connection.quote_table_name(klass.table_name.to_s) + "." + connection.quote_column_name(field.to_s)
      column_name = "(#{column_name} >> #{offset*word_size} & #{2**word_size - 1})" if offset
      column_name
    end

    # This method construct join statement for a key value table
    # It assume the following table structure
    #  +----------+  +---------+ +--------+
    #  | main     |  | value   | | key    |
    #  | main_pk  |  | main_fk | |        |
    #  |          |  | key_fk  | | key_pk |
    #  +----------+  +---------+ +--------+
    # uniq name for the joins are needed in case that there is more than one condition
    # on different keys in the same query.
    def construct_join_sql(key_relation, num )
      join_sql = ""
      connection = klass.connection
      key = key_relation.to_s.singularize.to_sym

      key_table = klass.reflections[key].table_name
      value_table = klass.table_name.to_s

      value_table_fk_key, key_table_pk = reflection_keys(klass.reflections[key])

      main_reflection = definition.klass.reflections[relation]
      if main_reflection
        main_table = definition.klass.table_name
        main_table_pk, value_table_fk_main = reflection_keys(definition.klass.reflections[relation])

        join_sql = "\n  INNER JOIN #{connection.quote_table_name(value_table)} #{value_table}_#{num} ON (#{main_table}.#{main_table_pk} = #{value_table}_#{num}.#{value_table_fk_main})"
        value_table = " #{value_table}_#{num}"
      end
      join_sql += "\n INNER JOIN #{connection.quote_table_name(key_table)} #{key_table}_#{num} ON (#{key_table}_#{num}.#{key_table_pk} = #{value_table}.#{value_table_fk_key}) "

      return join_sql
    end

    # This method construct join statement for a key value table
    # It assume the following table structure
    #  +----------+  +---------+
    #  | main     |  | key     |
    #  | main_pk  |  | value   |
    #  |          |  | main_fk |
    #  +----------+  +---------+
    # uniq name for the joins are needed in case that there is more than one condition
    # on different keys in the same query.
    def construct_simple_join_sql( num )
      connection = klass.connection
      key_value_table = klass.table_name

      main_table = definition.klass.table_name
      main_table_pk, value_table_fk_main = reflection_keys(definition.klass.reflections[relation])

      join_sql = "\n  INNER JOIN #{connection.quote_table_name(key_value_table)} #{key_value_table}_#{num} ON (#{connection.quote_table_name(main_table)}.#{connection.quote_column_name(main_table_pk)} = #{key_value_table}_#{num}.#{connection.quote_column_name(value_table_fk_main)})"
      return join_sql
    end

    def reflection_keys(reflection)
      pk = reflection.klass.primary_key
      fk = reflection.options[:foreign_key]
      # activerecord prior to 3.1 doesn't respond to foreign_key method and hold the key name in the reflection primary key
      fk ||= reflection.respond_to?(:foreign_key) ? reflection.foreign_key : reflection.primary_key_name
      reflection.macro == :belongs_to ? [fk, pk] : [pk, fk]
    end

    def reflection_conditions(reflection)
      return unless reflection
      conditions = reflection.options[:conditions]
      conditions ||= "#{reflection.options[:source]}_type = '#{reflection.options[:source_type]}'" if reflection.options[:source] && reflection.options[:source_type]
      conditions ||= "#{reflection.try(:foreign_type)} = '#{reflection.klass}'" if  reflection.options[:polymorphic]
      " AND #{conditions}" if conditions
    end

    def to_ext_method_sql(key, operator, value, &block)
      raise ScopedSearch::QueryNotSupported, "'#{definition.klass}' doesn't respond to '#{ext_method}'" unless definition.klass.respond_to?(ext_method)
      conditions = definition.klass.send(ext_method.to_sym,key, operator, value) rescue {}
      raise ScopedSearch::QueryNotSupported, "external method '#{ext_method}' should return hash" unless conditions.kind_of?(Hash)
      sql = ''
      conditions.map do |notification, content|
        case notification
          when :include then yield(:include, content)
          when :joins then yield(:joins, content)
          when :conditions then sql = content
          when :parameter then content.map{|c| yield(:parameter, c)}
        end
      end
      return sql
    end
  end
  Definition::Field.send(:include, QueryBuilder::Field)
end
