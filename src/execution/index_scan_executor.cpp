//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() { 
    
    ExecutorContext * ctx = GetExecutorContext();
    index_oid_t iot = plan_->GetIndexOid();
    Catalog * catalog = ctx->GetCatalog();
    indexinfo_ = catalog->GetIndex(iot);
    tableinfo_ = catalog->GetTable(indexinfo_->table_name_);
    BPlusTreeIndexForOneIntegerColumn* b_plus_tree_index= dynamic_cast<BPlusTreeIndexForOneIntegerColumn*>(indexinfo_->index_.get());    
    cursor_ = b_plus_tree_index->GetBeginIterator();
    end_ = b_plus_tree_index->GetEndIterator();

}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if ( cursor_==end_ ) {
        return false;
    }

    auto kv_map = *cursor_;
    IntegerValueType value = kv_map.second;
    *rid = value;
    tableinfo_->table_->GetTuple(value, tuple, GetExecutorContext()->GetTransaction());
    ++cursor_;
    return true;
}
    
    
}  // namespace bustub
