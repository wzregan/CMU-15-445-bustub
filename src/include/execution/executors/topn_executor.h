//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
class CMP{
public:
    CMP(){}
public:
    bool operator()(std::pair<std::vector<std::pair<Value, OrderByType>>, Tuple> a, std::pair<std::vector<std::pair<Value, OrderByType>>, Tuple> b){
      for (unsigned int i = 0; i < a.first.size(); i++) {
        OrderByType ordertype_ = a.first[i].second;
        Value va = a.first[i].first;
        Value vb = b.first[i].first;
        if (ordertype_==OrderByType::DESC) {
            if (va.CompareEquals(vb)==CmpBool::CmpTrue){
              continue;
            }
            return va.CompareLessThan(vb)==CmpBool::CmpTrue;
        }else if (ordertype_==OrderByType::DEFAULT || ordertype_==OrderByType::ASC) {
            if (va.CompareEquals(vb)==CmpBool::CmpTrue){
              continue;
            }
            return va.CompareGreaterThan(vb)==CmpBool::CmpTrue;
        }
      }
      return true;
    }
  };

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The topn plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);
  /** Initialize the topn */
  void Init() override;
  /**
   * Yield the next tuple from the topn.
   * @param[out] tuple The next tuple produced by the topn
   * @param[out] rid The next tuple RID produced by the topn
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the topn */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The topn plan node to be executed */
  unsigned int cursor_ = 0;
  std::unique_ptr<AbstractExecutor> child_executor_;
  const TopNPlanNode *plan_;
  std::priority_queue<std::pair<std::vector<std::pair<Value, OrderByType>>, Tuple>, std::vector<std::pair<std::vector<std::pair<Value, OrderByType>>, Tuple>>, CMP> buffer_;
};
}  // namespace bustub
