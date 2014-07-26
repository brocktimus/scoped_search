class ScopedSearch::QueryBuilder
  module AST
    module LeafNode
      def to_sql(builder, definition, &block)
        # for boolean fields allow a short format (example: for 'enabled = true' also allow 'enabled')
        field = definition.field_by_name(value)
        if field && field.set? && field.complete_value.values.include?(true)
          key = field.complete_value.map{|k,v| k if v == true}.compact.first
          return builder.set_test(field, :eq, key, &block)
        end
        # Search keywords found without context, just search on all the default fields
        fragments = definition.default_fields_for(value).map do |field|
          builder.sql_test(field, field.default_operator, value,'', &block)
        end

        case fragments.length
          when 0 then nil
          when 1 then fragments.first
          else "#{fragments.join(' OR ')}"
        end
      end
    end
  end
end
QueryLanguage::AST::LeafNode.send(:include, QueryBuilder::AST::LeafNode)
