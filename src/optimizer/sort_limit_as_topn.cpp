#include "optimizer/optimizer.h"
#include "execution/plans/topn_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetChildren().size() > 0 && optimized_plan->GetType() == PlanType::Limit) {
    auto child_plan = optimized_plan->GetChildAt(0);
    const LimitPlanNode * limit_plan_node = dynamic_cast<const LimitPlanNode *>(optimized_plan.get());
    const SortPlanNode * sort_plan_node = dynamic_cast<const SortPlanNode *>(child_plan.get());
    if (child_plan->GetType() == PlanType::Sort) {
      // do something 
      std::vector<std::pair<OrderByType, AbstractExpressionRef>>(sort_plan_node->GetOrderBy());
      auto child_plan_new = child_plan->CloneWithChildren(child_plan->GetChildren());
      
      auto topN_plan_node = std::make_shared<TopNPlanNode>(std::make_shared<const Schema>(limit_plan_node->OutputSchema()), sort_plan_node->GetChildPlan(),
                    std::vector<std::pair<OrderByType, AbstractExpressionRef>>(sort_plan_node->GetOrderBy()),
                    limit_plan_node->GetLimit());
      return topN_plan_node;
    }
  }
  return optimized_plan;
}

}  // namespace bustub
