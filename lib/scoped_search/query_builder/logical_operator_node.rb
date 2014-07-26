class ScopedSearch::QueryBuilder
  module AST
    module LogicalOperatorNode
      def to_sql(builder, definition, &block)
        fragments = children.map { |c| c.to_sql(builder, definition, &block) }.map { |sql| "(#{sql})" unless sql.blank? }.compact
        fragments.empty? ? nil : "#{fragments.join(" #{operator.to_s.upcase} ")}"
      end
    end
  end
end
QueryLanguage::AST::LogicalOperatorNode.send(:include, QueryBuilder::AST::LogicalOperatorNode)
