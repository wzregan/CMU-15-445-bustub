#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
        
    }

void SortExecutor::Init() { 
    auto& order_exps = plan_->GetOrderBy();
    Tuple read_tuple;
    RID read_rid;
    child_executor_->Init();
    buffer_.clear();
    while (child_executor_->Next(&read_tuple, &read_rid)) {
        std::vector<std::pair<Value, OrderByType>> order_values;
        for (auto order_exp: order_exps) {
            OrderByType ot = order_exp.first;
            Value v = order_exp.second->Evaluate(&read_tuple, child_executor_->GetOutputSchema());
            order_values.push_back({v, ot});
        }
        buffer_.push_back({{order_values},read_tuple});
    }
    std::sort(buffer_.begin(), buffer_.end(), CMP());
    cursor_ = 0;
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (cursor_ >= buffer_.size()) {
        return false;
    }
    *tuple = buffer_[cursor_].second;
    cursor_++;
    return true;
 }
}  // namespace bustub
