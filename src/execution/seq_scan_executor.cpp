//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan){
}
SeqScanExecutor::~SeqScanExecutor() {
}

void SeqScanExecutor::Init() { 
    ExecutorContext * ec = this->GetExecutorContext();
    
    Catalog * catalog = ec->GetCatalog();
    
    table_oid_t table_oid = plan_->GetTableOid();

    TableInfo * table_info = catalog->GetTable(table_oid);
    cursor_ = std::make_shared<TableIterator>(table_info->table_->Begin(ec->GetTransaction()));
    end_ = std::make_shared<TableIterator>(table_info->table_->End());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if ((*cursor_)==(*end_)) {
        return false;
    }
    *tuple = (*(*cursor_));
    *rid = tuple->GetRid();
    (*cursor_)++;
    return true; 
}



}  // namespace bustub
