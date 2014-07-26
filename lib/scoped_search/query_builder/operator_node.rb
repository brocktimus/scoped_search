class ScopedSearch::QueryBuilder
  module AST
    module OperatorNode

      # Returns an IS (NOT) NULL SQL fragment
      def to_null_sql(builder, definition, &block)
        field = definition.field_by_name(rhs.value)
        raise ScopedSearch::QueryNotSupported, "Field '#{rhs.value}' not recognized for searching!" unless field

        if field.key_field
          yield(:parameter, rhs.value.to_s.sub(/^.*\./,''))
        end
        case operator
          when :null    then "#{field.to_sql(builder, &block)} IS NULL"
          when :notnull then "#{field.to_sql(builder, &block)} IS NOT NULL"
        end
      end

      # No explicit field name given, run the operator on all default fields
      def to_default_fields_sql(builder, definition, &block)
        raise ScopedSearch::QueryNotSupported, "Value not a leaf node" unless rhs.kind_of?(ScopedSearch::QueryLanguage::AST::LeafNode)

        # Search keywords found without context, just search on all the default fields
        fragments = definition.default_fields_for(rhs.value, operator).map { |field|
                        builder.sql_test(field, operator, rhs.value,'', &block) }.compact

        case fragments.length
          when 0 then nil
          when 1 then fragments.first
          else "#{fragments.join(' OR ')}"
        end
      end

      # Explicit field name given, run the operator on the specified field only
      def to_single_field_sql(builder, definition, &block)
        raise ScopedSearch::QueryNotSupported, "Field name not a leaf node" unless lhs.kind_of?(ScopedSearch::QueryLanguage::AST::LeafNode)
        raise ScopedSearch::QueryNotSupported, "Value not a leaf node"      unless rhs.kind_of?(ScopedSearch::QueryLanguage::AST::LeafNode)

        # Search only on the given field.
        field = definition.field_by_name(lhs.value)
        raise ScopedSearch::QueryNotSupported, "Field '#{lhs.value}' not recognized for searching!" unless field
        builder.sql_test(field, operator, rhs.value,lhs.value, &block)
      end

      # Convert this AST node to an SQL fragment.
      def to_sql(builder, definition, &block)
        if operator == :not && children.length == 1
          builder.to_not_sql(rhs, definition, &block)
        elsif [:null, :notnull].include?(operator)
          to_null_sql(builder, definition, &block)
        elsif children.length == 1
          to_default_fields_sql(builder, definition, &block)
        elsif children.length == 2
          to_single_field_sql(builder, definition, &block)
        else
          raise ScopedSearch::QueryNotSupported, "Don't know how to handle this operator node: #{operator.inspect} with #{children.inspect}!"
        end
      end
    end
  end
end
QueryLanguage::AST::OperatorNode.send(:include, QueryBuilder::AST::OperatorNode)
