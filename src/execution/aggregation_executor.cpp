//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child)) {}

void AggregationExecutor::Init() {
    child_->Init();
    Tuple scan_tuple;
    RID scan_rid;
    // 先读取所有数据
    [[maybe_unused]] const std::vector<bustub::AbstractExpressionRef> & refs = plan_->GetAggregates();
    [[maybe_unused]] const std::vector<bustub::AggregationType> & types = plan_->GetAggregateTypes();
    [[maybe_unused]] const std::vector<bustub::AbstractExpressionRef> & groupby_refs = plan_->GetGroupBys();
    aggregation_hash_tab_ = std::make_shared<SimpleAggregationHashTable>(refs, types);
    [[maybe_unused]] const bustub::Schema & child_schema = child_->GetOutputSchema();
    [[maybe_unused]] const Schema & output_schema = plan_->OutputSchema();
    int count = 0;
    while (child_->Next(&scan_tuple, &scan_rid)) {
        std::vector<Value> aggregate_key_vec;
        for (const AbstractExpressionRef &groupby_ref : groupby_refs) {
            Value groupby_key = groupby_ref->Evaluate(&scan_tuple, child_schema);
            aggregate_key_vec.push_back(groupby_key);
        }

        std::vector<Value> aggregate_val_vec;
        for (const AbstractExpressionRef &agg_val_ref : refs) {
            
            Value agg_val = agg_val_ref->Evaluate(&scan_tuple, child_schema);
            aggregate_val_vec.push_back(agg_val);
        }
        AggregateKey groupby_key_agg = {aggregate_key_vec};
        AggregateValue aggregate_val = {aggregate_val_vec};
        aggregation_hash_tab_->InsertCombine(groupby_key_agg, aggregate_val);
        count++;
    }
    if ( count==0 && groupby_refs.size() == 0) {
        aggregation_hash_tab_->InitOneNullLine();
    }

    cursor_ = std::make_shared<SimpleAggregationHashTable::Iterator>(aggregation_hash_tab_->Begin());
    end_ = std::make_shared<SimpleAggregationHashTable::Iterator>(aggregation_hash_tab_->End());;
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if ((*cursor_)==(*end_)) {
        return false;
    }
    const Schema & schema = plan_->OutputSchema();
    AggregateKey key = cursor_->Key();
    AggregateValue val = cursor_->Val();
    ++(*cursor_);
    std::vector<Value> values;
    for (const Value &v: key.group_bys_) {
        values.push_back(v);
    }
    for (const Value &v: val.aggregates_) {
        values.push_back(v);
    }
    Tuple agg_tuple(values, &schema);
    *tuple = agg_tuple;
    return true; 
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
