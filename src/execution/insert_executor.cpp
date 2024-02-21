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

    


}

void InsertExecutor::Init() {  
    cursor_ = 0;
    table_oid_t insert_tab_oid = plan_->TableOid();
    Catalog * catalog = GetExecutorContext()->GetCatalog();
    table_info_ = catalog->GetTable(insert_tab_oid);
    index_infos_ = catalog->GetTableIndexes(table_info_->name_);
    child_executor_->Init();
    exit_ = false;
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
        table_info_->table_->InsertTuple(insert_tuple, &insert_rid, GetExecutorContext()->GetTransaction());
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
 