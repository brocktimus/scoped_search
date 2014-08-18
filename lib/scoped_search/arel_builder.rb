# What does the profile do?

module ScopedSearch
  class ArelBuilder
    def self.build_query(definition, query = '', options = {})
      builder_klass = class_for(definition)
      query = QueryLanguaage::Compiler.parse(query)
      builder_klass.new(definition, query, options)
    end

    # Loads the QueryBuilder class for the connection of the given definition.
    # If no specific adapter is found, the default QueryBuilder class is returned.
    def self.class_for(definition)
      connection_class = definition.klass.connection.class.name.split('::').last
      self.const_get(connection_class) rescue self
    end

    def initialize(definition, ast, options = {})
      @definition, @ast = definition, ast
      @definition.profile = options.delete(:profile)
      @ordering = options.delete(:ordering)
    end

    def required_joins
    end

    def where_conditions
    end

    def ordering
    end

  private

    def model
      definition.klass
    end

    def visit(node)
    end

    def visit_LogicalOperatorNode(node)
      node.children.reduce(node.operator.to_proc)
    end

    def visit_OperatorNode(node)
      if node.operator == :not && node.children.length == 1
        visit_OperatorNode_not(node)
      elsif node.operator == :null
        visit_OperatorNode_null(node)
      elsif node.operator == :notnull
        visit_OperatorNode_notnull(node)
      elsif node.children.length == 1
        visit_OperatorNode_default_fields(node)
      elsif node.children.length == 2
        visit_OperatorNode_exact_field(node)
      else
        raise ScopedSearch::QueryNotSupported, "Don't know how to handle this operator node: #{node.operator.inspect} with #{node.children.inspect}!"
      end
    end

    def visit_OperatorNode_not(node)
      visit(node).not
    end

    def visit_OperatorNode_null(node)
      field_for(node.rhs.value).eq(nil)
    end

    def visit_OperatorNode_notnull(node)
      field_for(node.rhs.value).not_eq(nil)
    end

    def visit_OperatorNode_exact_field(node)
      # TODO: Dispatch via hash to visit_OperatorNode_operator
      send("visit_OperatorNode_#{node.operator}", node)
    end

    def visit_OperatorNode_default_fields(node)
      raise NotImplementedError "Instead convert it into an OR and multile exact nodes"
    end

    def visit_OperatorNode_like(node)
      wrapped_value = optionally_wrap_value_in_wildcards(node.rhs.value)
      field_for(node.lhs.value).matches(wrapped_value)
    end

    def visit_OperatorNode_unlike(node)
      wrapped_value = optionally_wrap_value_in_wildcards(node.rhs.value)
      field_for(node.lhs.value).does_not_match(wrapped_value)
    end

    # NOTE: Most of this logic dates back to initial string based implementation
    def visit_OperatorNode_datetime(node)
      operator = node.operator
      arel_field = field_for(node.lhs.value)
      field = definition.field_by_name(node.lhs.value)
      timestamp = definition.parse_temporal(node.rhs.value)
      return unless timestamp

      timestamp = timestamp.to_date if field.date?

      # Check for the case that a date-only value is given as search keyword,
      # but the field is of datetime type. Change the comparison to return
      # more logical results.
      if field.datetime?
        span = 1.minute if value =~ /\A\s*\d+\s+\bminutes?\b\s+\bago\b\s*\z/i
        span ||= (timestamp.day_fraction == 0) ? 1.day : 1.hour

        if [:eq, :ne].include? operator
          # Instead of looking for an exact (non-)match, look for dates that
          # fall inside/outside the range of timestamps of that day.
          
          range = timestamp...(timestamp + span)
          output = arel_field.in(range)
          output = output.not if operator == :ne
          return output
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

      arel_field.send(operator, timestamp)
    end

    def visit_OperatorNode_set(node)
      operator = node.operator
      field = definition.field_by_name(node.lhs.value)
      value = field.complete_value.with_indifferent_access.fetch(node.rhs.value) do
        raise ScopedSearch::QueryNotSupported, "'#{field.field}' should be one of '#{field.complete_value.keys.join(', ')}', but the query was '#{value}'"
      end
      raise ScopedSearch::QueryNotSupported, "Operator '#{operator}' not supported for '#{field.field}'" unless [:eq, :ne].include?(operator)
      negated = (operator == :ne)

      if [true, false].include?(value) and field.numerical?
        # NOTE: This code makes very little sense to me...
        # NOTE: There was a branch about non numeric fields, seemed redundant
        operator = (value == true) ? :gt : :eq
        value = 0
      end

      output = field_for(node.lhs.value).send(operator, value)
      output.not if negated
    end

    def visit_OperatorNode_has_many(node)
    end

    def visit_OperatorNode_has_many_through(node)
    end

    def visit_OperatorNode_everything_else(node)
      raise NotImplemented "We need to check the mapping of stuff"
      field_for(node.lhs.value).send(node.operator, node.rhs.value)
    end

    def optionally_wrap_value_in_wildcards(value, wildcard_inputs = %w[% *])
      (value !~ /^\%|\*/ && value !~ /\%|\*$/) ? "%#{value}%" : value.tr_s('%*', '%')
    end

    def visit_OperatorNode_in(node)
      exploded_value = explode_stringy_array(node.rhs.value)
      field_for(node.lhs.value).in(exploded_value)
    end

    def visit_OperatorNode_notin(node)
      exploded_value = explode_stringy_array(node.rhs.value)
      field_for(node.lhs.value).not_in(exploded_value)
    end

    def explode_stringy_array(value, around = ',')
      value.split(',')
    end
  end
end
