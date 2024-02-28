//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"
namespace bustub {

// 注意：这个执行器只有当inner有索引的时候才会执行，outer表一定是顺序扫描、inner表通过索引扫描
NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
  ExecutorContext *ctx = GetExecutorContext();
  index_oid_t iot = plan_->GetIndexOid();
  Catalog *catalog = ctx->GetCatalog();
  indexinfo_ = catalog->GetIndex(iot);
  tableinfo_ = catalog->GetTable(indexinfo_->table_name_);
  b_plus_tree_index_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(indexinfo_->index_.get());
  child_executor_->Init();
}
auto NestIndexJoinExecutor::FetchTupleByIndex(Tuple key, std::vector<Tuple> *result) -> bool {
  std::vector<RID> result_rids;
  b_plus_tree_index_->ScanKey(key, &result_rids, GetExecutorContext()->GetTransaction());
  int count = 0;
  for (RID rid : result_rids) {
	Tuple fetch_tuple;
	tableinfo_->table_->GetTuple(rid, &fetch_tuple, GetExecutorContext()->GetTransaction());
    result->push_back(std::move(fetch_tuple));
    count++;
  }
  if (count > 0) {
    return true;
  }
  return false;
}

auto NestIndexJoinExecutor::GenerateNullTuple(const Schema &schema) -> Tuple {
  std::vector<Value> null_value;
  for (unsigned int i = 0; i < schema.GetColumnCount(); i++) {
    Column column = schema.GetColumn(i);
    null_value.push_back(ValueFactory::GetNullValueByType(column.GetType()));
  }
  return Tuple(null_value, &schema);
}

auto NestIndexJoinExecutor::GetJoinTuple(const Tuple &left_tuple, const Tuple &right_tuple) -> Tuple {
  const Schema &out_put_schema = plan_->OutputSchema();
  std::vector<Value> join_values;
  std::unordered_map<std::string, Value> value_map;
  const Schema &left_schema = child_executor_->GetOutputSchema();
  const Schema &right_schema = *plan_->inner_table_schema_.get();
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

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 1. 顺序读取tuple（优先）
  Tuple outer_tuple;
  RID outer_rid;
  
  const Schema &outer_schema = child_executor_->GetOutputSchema();
  auto predict = plan_->KeyPredicate();
  Schema *key_schema = b_plus_tree_index_->GetKeySchema();
  while (joined_buffer.size() == 0) {
	bool state = child_executor_->Next(&outer_tuple, &outer_rid);
	if (!state) {
		return false;
	}
    Value key_value = predict->Evaluate(&outer_tuple, outer_schema);
    std::vector<Tuple> result;
    child_executor_->GetOutputSchema();
    bool fetch_result = FetchTupleByIndex(Tuple({key_value}, key_schema), &result);
    if (!fetch_result && plan_->GetJoinType() == JoinType::LEFT) {
      joined_buffer.push_back(GetJoinTuple(outer_tuple, GenerateNullTuple(*plan_->inner_table_schema_.get())));
      break;
    }
    for (Tuple &inner_tuple : result) {
		joined_buffer.push_back(GetJoinTuple(outer_tuple, inner_tuple));
    }
  }
  if (joined_buffer.size() > 0) {
    *tuple = joined_buffer.back();
    joined_buffer.pop_back();
    return true;
  }
  
  return false;
}

}  // namespace bustub
