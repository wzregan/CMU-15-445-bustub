//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"
#define JOIN_BUFFER_SIZE 1
namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  count_=0;
  success_join_buffer = std::vector<std::list<Tuple>>(JOIN_BUFFER_SIZE, std::list<Tuple>());
}

auto NestedLoopJoinExecutor::GetJoinTuple(const Tuple &left_tuple, const Tuple &right_tuple) -> Tuple {
  const Schema &out_put_schema = plan_->OutputSchema();
  std::vector<Value> join_values;
  std::unordered_map<std::string, Value> value_map;
  const Schema &left_schema = plan_->GetLeftPlan()->OutputSchema();
  const Schema &right_schema = plan_->GetRightPlan()->OutputSchema();
  for (unsigned int i = 0; i < left_schema.GetColumnCount(); i++) {
    value_map.insert({left_schema.GetColumn(i).GetName(), left_tuple.GetValue(&left_schema, i)});
  }

  for (unsigned int i = 0; i < right_schema.GetColumnCount(); i++) {
    value_map.insert({right_schema.GetColumn(i).GetName(), right_tuple.GetValue(&right_schema, i)});
  }

  for (unsigned int i = 0; i < out_put_schema.GetColumnCount(); i++) {
    Column column = out_put_schema.GetColumn(i);
    join_values.push_back(value_map[column.GetName()]);
  }
  Tuple joined_tuple(join_values, &out_put_schema);
  return joined_tuple;
}

auto NestedLoopJoinExecutor::LoadNextLeftBuffer() -> bool {
  join_buffer_.clear();
  matched_join.clear();
  Tuple read_tuple;
  RID read_rid;
  while (join_buffer_.size() < JOIN_BUFFER_SIZE && left_executor_->Next(&read_tuple, &read_rid)) {
    join_buffer_.push_back(read_tuple);
    matched_join.push_back(false);
  }
  if (join_buffer_.empty()) {
    return false;
  }
  return true;
}

auto NestedLoopJoinExecutor::GenerateNullTuple(const Schema &schema) -> Tuple {
  std::vector<Value> null_value;
  for (unsigned int i = 0; i < schema.GetColumnCount(); i++) {
    Column column = schema.GetColumn(i);
    null_value.push_back(ValueFactory::GetNullValueByType(column.GetType()));
  }
  return Tuple(null_value, &schema);
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple right_tuple;
  RID right_rid;
  [[maybe_unused]] auto &predict = plan_->Predicate();
  const Schema &left_schema = plan_->GetLeftPlan()->OutputSchema();
  const Schema &right_schema = plan_->GetRightPlan()->OutputSchema();
  Tuple left_tuple;
  while (count_ == 0) {
    // 读inner_tables
    bool state = right_executor_->Next(&right_tuple, &right_rid);

    // 如果内表读完了
    if (!state) {
      // 如果是left join，那么需要将没有匹配上的给加入到查询结果中
      if (plan_->GetJoinType() == JoinType::LEFT) {
        for (unsigned int k = 0; k < join_buffer_.size(); k++) {
          if (matched_join[k]) {
            continue;
          }
          
          Tuple no_match_tuple = GetJoinTuple(join_buffer_[k], GenerateNullTuple(right_schema));
          success_join_buffer[k].push_back(no_match_tuple);
          count_++;
        }
      }
      bool load_state = LoadNextLeftBuffer();
      right_executor_->Init();
      if (!load_state) {
        break;
      }
      continue;
    }
    
    // 开始进行连接操作
    for (unsigned int i = 0; i < join_buffer_.size(); i++) {
      left_tuple = join_buffer_[i];
      Value is_join = predict.EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema);
      Value True_Val(BOOLEAN, true);
      is_join.GetTypeId();
      if (is_join.CompareEquals(True_Val) == CmpBool::CmpTrue) {
        Tuple join_tuple = GetJoinTuple(left_tuple, right_tuple);
        success_join_buffer[i].push_back(join_tuple);
        matched_join[i] = true;
        count_++;
      }
    }
  }

  for (unsigned int k = 0; k < success_join_buffer.size(); k++) {
    if (success_join_buffer[k].size() == 0) {
      continue;
    } else {
      *tuple = success_join_buffer[k].front();
      success_join_buffer[k].pop_front();
      count_--;
      return true;
    }
  }
  return false;

}


}  // namespace bustub
