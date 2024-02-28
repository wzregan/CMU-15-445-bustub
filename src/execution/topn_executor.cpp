#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), child_executor_(std::move(child_executor)), plan_(plan) {



    }

void TopNExecutor::Init() { 
    auto& order_exps = plan_->GetOrderBy();
    Tuple read_tuple;
    RID read_rid;
    child_executor_->Init();
    while (child_executor_->Next(&read_tuple, &read_rid)) {
        std::vector<std::pair<Value, OrderByType>> order_values;
        for (auto order_exp: order_exps) {
            OrderByType ot = order_exp.first;
            Value v = order_exp.second->Evaluate(&read_tuple, child_executor_->GetOutputSchema());
            order_values.push_back({v, ot});
        }
        buffer_.push({{order_values},read_tuple});
    }
    cursor_ = 0;

}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (cursor_ >= plan_->GetN() || buffer_.empty()) {
        return false;
    }
    *tuple = buffer_.top().second;
    buffer_.pop();
    cursor_++;
    return true;

}

}  // namespace bustub
