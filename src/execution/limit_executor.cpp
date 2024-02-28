//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() { 
    child_executor_->Init();
    cursor_ = 0;
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (cursor_==plan_->GetLimit()) {
        return false;
    }
    Tuple read_tuple;
    RID read_rid;
    bool state = child_executor_->Next(&read_tuple, &read_rid);
    if (!state) {
        return false;
    }
    *tuple = read_tuple;
    *rid = read_rid;
    cursor_++;
    return true; 
 }

}  // namespace bustub
