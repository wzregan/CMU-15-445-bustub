//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)){


    }

void DeleteExecutor::Init() { 
    Catalog * catalog  = exec_ctx_->GetCatalog();
    tab_info_ = catalog->GetTable(plan_->TableOid());
    child_executor_->Init();
    idx_infos_ = catalog->GetTableIndexes(tab_info_->name_);
    exit_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
    if (exit_) {
        return false;
    }
    int delete_count = 0;
    Tuple deleted_tuple = {};
    RID deleted_rid = {};
    while (child_executor_->Next(&deleted_tuple, &deleted_rid)) {
        tab_info_->table_->MarkDelete(deleted_rid, exec_ctx_->GetTransaction());
        delete_count++;
        // 然后更新索引树
        for  (auto index: idx_infos_) {
            // 1. 获取索引元数据
            IndexMetadata * index_meta = index->index_->GetMetadata();
            // 2. 获取索引的列数
            int index_column_count = index_meta->GetIndexColumnCount();
            std::vector<Value> index_values;
            // 3. 得到索引的KeySchema
            Schema * key_schema = index_meta->GetKeySchema();
            for (int i = 0; i < index_column_count; i++) {
                // 4. 获取建立索引的那个Column
                Column column = key_schema->GetColumn(i);
                // 5. 然后获取column在表中的列
                
                int col_idx = tab_info_->schema_.GetColIdx(column.GetName());
                // 6. 然后获取值
                Value v = deleted_tuple.GetValue(&tab_info_->schema_, col_idx);
                index_values.push_back(v);
            }
            // 7. 插入到索引树里
            auto delete_key = Tuple(index_values, key_schema);
            index->index_->DeleteEntry(delete_key, *rid, GetExecutorContext()->GetTransaction());
        }

    }
    // 要返回删除了多少条
    Value deleted_row_count = Value(INTEGER, delete_count);
    std::vector<bustub::Value> values{deleted_row_count};
    Schema schema = Schema(std::vector<Column>{Column("deleted_rows_count", INTEGER)});
    *tuple = Tuple(values, &schema);
    exit_ = true;
    return true; 
}

}  // namespace bustub
