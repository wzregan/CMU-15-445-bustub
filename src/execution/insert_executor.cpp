//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"
#include "execution/plans/values_plan.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  lock_manager_ = GetExecutorContext()->GetLockManager();
  txn = GetExecutorContext()->GetTransaction();
}

void InsertExecutor::Init() {  
    cursor_ = 0;
    table_oid_t insert_tab_oid = plan_->TableOid();
    Catalog * catalog = GetExecutorContext()->GetCatalog();
    table_info_ = catalog->GetTable(insert_tab_oid);
    index_infos_ = catalog->GetTableIndexes(table_info_->name_);
    child_executor_->Init();
    exit_ = false;

    // 需要对整表加上IX锁
    // 事务的隔离方式是READ_COMMITTED，在进行插入的时候需要对row进行上锁，知道事务结束才解锁
    // 这样别的时候就读不出来这一行的数据了
    // 同样的道理，为了解决幻读：A查完数据，B插入数据，A在查数据发现数据多了，为了解决这个问题，A查数据必须加读锁，且读锁得等十五结束后才解开
    // 所有三种隔离方式都要对表加IX锁
    if (!lock_manager_->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_)) {
        // 如果加锁失败，则需要终止事务
    }

}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
    if (exit_) {
        return false;
    }

    Tuple insert_tuple{};
    std::vector<Tuple> insert_tuples;
    // 先执行子执行器，将需要插入的数据都聚集到insert_tuples里面
    while(child_executor_->Next(&insert_tuple, rid)) {
        insert_tuples.push_back(insert_tuple);
    }
    
    for (auto &insert_tuple: insert_tuples) {
        RID insert_rid = insert_tuple.GetRid();
        // 需要再插入之前加入行锁
        
        table_info_->table_->InsertTuple(insert_tuple, &insert_rid, GetExecutorContext()->GetTransaction());
        lock_manager_->LockRow(txn, LockManager::LockMode::EXCLUSIVE, table_info_->oid_, insert_rid);
        // 插入之后就解锁
        if (txn->GetIsolationLevel()==IsolationLevel::READ_UNCOMMITTED) {
            lock_manager_->UnlockRow(txn, table_info_->oid_, insert_rid);
        }
        
        // 根据事务不同来判断解锁的时机: READ_UNCOMMIT READ_COMMIT 需要提前解锁
        

        // 然后更新索引树
        
        for  (auto index: index_infos_) {
            // 1. 获取索引元数据
            IndexMetadata * index_meta = index->index_->GetMetadata();
            // 2. 获取索引的列数
            int index_column_count = index_meta->GetIndexColumnCount();
            std::vector<Value> index_values;
            for (int i = 0; i < index_column_count; i++) {
                // 3. 得到索引的KeySchema
                Schema * key_schema = index_meta->GetKeySchema();
                // 4. 获取建立索引的那个Column
                Column column = key_schema->GetColumn(i);
                // 5. 然后获取column在表中的列
                int col_idx = table_info_->schema_.GetColIdx(column.GetName());
                // 6. 然后获取值
                Value v = insert_tuple.GetValue(&table_info_->schema_, col_idx);
                index_values.push_back(v);
            }
            // 7. 插入到索引树里
            index->index_->InsertEntry(Tuple(index_values, index_meta->GetKeySchema()), insert_rid, GetExecutorContext()->GetTransaction());
        }
        
        cursor_++;
    }

    


    Value inserted_row_count = Value(INTEGER, cursor_);
    std::vector<bustub::Value> values{inserted_row_count};
    Schema schema = Schema(std::vector<Column>{Column("insert_rows_count", INTEGER)});
    *tuple = Tuple(values, &schema);
    exit_ = true;
    return true;
}

}  // namespace bustub
 