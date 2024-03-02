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
#include "concurrency/lock_manager.h"
namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan), lock_manager_(nullptr) {
    lock_manager_ = GetExecutorContext()->GetLockManager();
    txn = GetExecutorContext()->GetTransaction();
}
SeqScanExecutor::~SeqScanExecutor() {
}

void SeqScanExecutor::Init() { 
    ExecutorContext * ec = this->GetExecutorContext();
    
    Catalog * catalog = ec->GetCatalog();
    
    table_oid_t table_oid = plan_->GetTableOid();

    table_info = catalog->GetTable(table_oid);
    
    cursor_ = std::make_shared<TableIterator>(table_info->table_->Begin(ec->GetTransaction()));
    end_ = std::make_shared<TableIterator>(table_info->table_->End());
    // 1. 如果是READ_COMMITTED事务的隔离级别，需要对表加上IS锁
    // 2. 对于REPEATABLE_READ隔离级别，我们也许对表加上IS锁，与1唯一的区别在与锁的释放时机
    if (txn->GetIsolationLevel()==IsolationLevel::READ_COMMITTED || txn->GetIsolationLevel()==IsolationLevel::REPEATABLE_READ) {
        if (!lock_manager_->LockTable(txn, LockManager::LockMode::INTENTION_SHARED, table_info->oid_)) {
            // 如果加锁失败，则需要终止事务
        }
    }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if ((*cursor_)==(*end_)) {
        // 表中已经没有数据了，需要释放整张表的IS锁
        if (txn->GetIsolationLevel()==IsolationLevel::READ_COMMITTED) {
            if (!lock_manager_->UnlockTable(txn, table_info->oid_)) {
                // 如果解锁失败，则需要终止事务
            }
        }
        return false;
    }

    if (txn->GetIsolationLevel()==IsolationLevel::READ_COMMITTED || txn->GetIsolationLevel()==IsolationLevel::REPEATABLE_READ) {
        // 加锁，但是读完就释放
        lock_manager_->LockRow(txn, LockManager::LockMode::SHARED, table_info->oid_, (*cursor_)->GetRid());
    }
    *tuple = (*(*cursor_));
    *rid = tuple->GetRid();
    // 对表进行解锁
    if (txn->GetIsolationLevel()==IsolationLevel::READ_COMMITTED || txn->GetIsolationLevel()==IsolationLevel::REPEATABLE_READ) {
        // 加锁，但是读完就释放
        lock_manager_->UnlockRow(txn, table_info->oid_, (*cursor_)->GetRid());
    }
    (*cursor_)++;


    return true; 
}



}  // namespace bustub
