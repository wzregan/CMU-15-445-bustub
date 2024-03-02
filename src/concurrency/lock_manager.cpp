//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
namespace bustub {
auto LockManager::HasLockOnTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool{
	if (lock_mode==LockMode::EXCLUSIVE) {
		return txn->GetExclusiveTableLockSet()->count(oid) > 0;
	}
	else if (lock_mode==LockMode::SHARED) {
		return txn->GetSharedTableLockSet()->count(oid) > 0;
	}
	else if (lock_mode==LockMode::INTENTION_EXCLUSIVE) {
		return txn->GetIntentionExclusiveTableLockSet()->count(oid) > 0;
	}
	else if (lock_mode==LockMode::INTENTION_SHARED) {
		return txn->GetIntentionSharedTableLockSet()->count(oid) > 0;
	}
	else if (lock_mode==LockMode::SHARED_INTENTION_EXCLUSIVE) {
		return txn->GetSharedIntentionExclusiveTableLockSet()->count(oid) > 0;
	}
	return false;
}

auto LockManager::HasLockOnRow(Transaction *txn, LockMode lock_mode, const table_oid_t & oid, const RID &rid) -> bool{
	if (lock_mode==LockMode::EXCLUSIVE) {
	}
	else if (lock_mode==LockMode::SHARED) {
	}
	else if (lock_mode==LockMode::INTENTION_EXCLUSIVE) {
	}
	else if (lock_mode==LockMode::INTENTION_SHARED) {
	}
	else if (lock_mode==LockMode::SHARED_INTENTION_EXCLUSIVE) {
	}
	return false;
}


auto LockManager::AddLockModeCount(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> void {
	if (lock_mode == LockMode::EXCLUSIVE) {
		txn->GetExclusiveTableLockSet()->insert(oid);
	} else if (lock_mode == LockMode::SHARED) {
		txn->GetSharedTableLockSet()->insert(oid);
	} else if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
		txn->GetIntentionExclusiveTableLockSet()->insert(oid);
	} else if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
		txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
	}else if (lock_mode == LockMode::INTENTION_SHARED) {
		txn->GetIntentionSharedTableLockSet()->insert(oid);
	}
}

auto LockManager::CutDownLockModeCount(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> void {
	if (lock_mode == LockMode::EXCLUSIVE) {
		txn->GetExclusiveTableLockSet()->erase(oid);
	} else if (lock_mode == LockMode::SHARED) {
		txn->GetSharedTableLockSet()->erase(oid);
	} else if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
		txn->GetIntentionExclusiveTableLockSet()->erase(oid);
	} else if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
		txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
	}
}


auto LockManager::AddLockRowModeCount(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> void {
	if (lock_mode == LockMode::EXCLUSIVE) {
		auto iter = txn->GetExclusiveRowLockSet()->find(oid);
		if (iter==txn->GetExclusiveRowLockSet()->end()) {
			auto unorder_set = std::unordered_set<bustub::RID>();
			unorder_set.insert(rid);
			txn->GetExclusiveRowLockSet()->insert({oid, unorder_set});
		}else{
			iter->second.insert(rid);
		}
	} else if (lock_mode == LockMode::SHARED) {
		auto iter = txn->GetSharedRowLockSet()->find(oid);
		if (iter==txn->GetSharedRowLockSet()->end()) {
			auto unorder_set = std::unordered_set<bustub::RID>();
			unorder_set.insert(rid);
			txn->GetSharedRowLockSet()->insert({oid, unorder_set});
		}else{
			iter->second.insert(rid);
		}
	}

}

auto LockManager::CutDownRowLockModeCount(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> void {
	if (lock_mode == LockMode::EXCLUSIVE) {
		auto iter = txn->GetExclusiveRowLockSet()->find(oid);
		if (iter!=txn->GetExclusiveRowLockSet()->end()) {
			iter->second.erase(rid);
		}
	} else if (lock_mode == LockMode::SHARED) {
		auto iter = txn->GetSharedRowLockSet()->find(oid);
		if (iter!=txn->GetSharedRowLockSet()->end()) {
			iter->second.erase(rid);
		}
	}

}


auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  auto * request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
  // 如果事务已经获得了这个表的锁，那么我们直接返回即可
  if (HasLockOnTable(txn, lock_mode, oid)) {
	return true;
  }
  // 如果持有高级锁，去请求低级锁，则直接抛出错误
	// 1. X -> S IS SIX
  if ((lock_mode==LockMode::SHARED || lock_mode==LockMode::INTENTION_SHARED || lock_mode==LockMode::SHARED_INTENTION_EXCLUSIVE || lock_mode==LockMode::INTENTION_EXCLUSIVE) && HasLockOnTable(txn, LockMode::EXCLUSIVE, oid)) {
	throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }
	// 2. S -> IS IE
  else if ((lock_mode==LockMode::INTENTION_SHARED || lock_mode==LockMode::INTENTION_EXCLUSIVE) && HasLockOnTable(txn, LockMode::SHARED, oid)) {
	throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }
	// 3. IE -> IS S
  else if ((lock_mode==LockMode::INTENTION_SHARED || lock_mode==LockMode::SHARED) && HasLockOnTable(txn, LockMode::INTENTION_EXCLUSIVE, oid)) {
	throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }
	// 4. SIX -> S IS
  else if ((lock_mode==LockMode::SHARED || lock_mode==LockMode::INTENTION_SHARED || lock_mode==LockMode::SHARED_INTENTION_EXCLUSIVE || lock_mode==LockMode::INTENTION_EXCLUSIVE) && HasLockOnTable(txn, LockMode::SHARED_INTENTION_EXCLUSIVE, oid)) {
	throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }
  
  // 后面要请求队列进行更新，所以要加上锁
  table_lock_map_latch_.lock();
  // 1. 先判断表是否已经有了请求队列
  if (table_lock_map_.count(oid) == 0) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  // 已经有了请求队列，则加入到队列中
  table_lock_map_[oid]->request_queue_.push_back(request);

  for (auto iter = table_lock_map_[oid]->request_queue_.begin(); iter != table_lock_map_[oid]->request_queue_.end();
       ) {
	if (txn->GetState()==TransactionState::ABORTED) {
		return false;
	}
	// 如果这个事务还没被授予锁，则直接跳过
    if (!(*iter)->granted_) {
	  iter++;
      continue;
    }
	// 如果锁请求队列中已经有了这个事务的请求，则说明下面即将做的必然是锁升级！ 所以把这个锁删掉，然后重新请求一个升级锁
	if ((*iter) != request && (*iter)->txn_id_ == request->txn_id_) {
		CutDownLockModeCount(txn, (*iter)->lock_mode_, oid);
		auto old_iter = iter;
		iter++;
		delete *old_iter;
		table_lock_map_[oid]->request_queue_.erase(old_iter);
		continue;
	}
	
	// 如果这个事务的锁不兼容
    if (!IsLockCompatible((*iter)->lock_mode_, request->lock_mode_) && (*iter)->txn_id_!=request->txn_id_) {
      table_lock_map_latch_.unlock();
	  table_lock_map_[oid]->wait(txn);
	  table_lock_map_latch_.lock();
      iter = table_lock_map_[oid]->request_queue_.begin();
    }else {
		iter++;
	}
  }
  // 判断一下是否可以升级，不能升级的话another_request必为空
  txn->SetState(TransactionState::GROWING);
  request->granted_ = true;
  table_lock_map_latch_.unlock();
  // 更新事务所持有锁的记录
  AddLockModeCount(txn, lock_mode, oid);

  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // 1. 先判断表是否已经有了请求队列
  table_lock_map_latch_.lock();
  if (table_lock_map_.count(oid) == 0) {
    return false;
  }
  for (auto iter = table_lock_map_[oid]->request_queue_.begin(); iter != table_lock_map_[oid]->request_queue_.end();
       iter++) {
    if ( (*iter)->granted_ && (*iter)->txn_id_==txn->GetTransactionId()) {
	  CutDownLockModeCount(txn, (*iter)->lock_mode_, oid);
	  if (txn->GetState()==TransactionState::GROWING) {
		if ((*iter)->lock_mode_==LockMode::SHARED && (txn->GetIsolationLevel()==IsolationLevel::REPEATABLE_READ) ) {
			txn->SetState(TransactionState::SHRINKING);
		}else if ((*iter)->lock_mode_==LockMode::EXCLUSIVE) {
	  		txn->SetState(TransactionState::SHRINKING);
		}
	  }
	  delete *iter;
	  table_lock_map_[oid]->request_queue_.erase(iter);
      table_lock_map_[oid]->notify_one();
	  table_lock_map_latch_.unlock();
      return true;
    }
  }
  // 如果没有找到用户的请求...
  table_lock_map_latch_.unlock();
  return false;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  auto * request = new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid);

  // 判断有没有锁在此行上
  if (HasLockOnRow(txn, lock_mode, oid, rid)) {
	return true;
  }

  // 在row进行加锁之前，先判读表有没锁
  if (lock_mode==LockMode::EXCLUSIVE && !(txn->IsTableExclusiveLocked(oid) 
  		|| txn->IsTableSharedIntentionExclusiveLocked(oid) 
		|| txn->IsTableIntentionExclusiveLocked(oid))) {
	return false;
  }

  if (lock_mode==LockMode::SHARED && !(txn->IsTableSharedLocked(oid) 
  		|| txn->IsTableIntentionSharedLocked(oid) 
		|| txn->IsTableSharedIntentionExclusiveLocked(oid) 
		|| txn->IsTableExclusiveLocked(oid)
		|| txn->IsTableIntentionExclusiveLocked(oid))) {
	return false;
  }
  // 后面要请求队列进行更新，所以要加上锁
  row_lock_map_latch_.lock();
  // 1. 先判断行是否已经有了请求队列
  if (row_lock_map_.count(rid) == 0) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  // 已经有了请求队列，则加入到队列中
  row_lock_map_[rid]->request_queue_.push_back(request);

  for (auto iter = row_lock_map_[rid]->request_queue_.begin(); iter != row_lock_map_[rid]->request_queue_.end();
       ) {
	// 如果事务已经被终止.....
	if (txn->GetState()==TransactionState::ABORTED && (*iter)->txn_id_ == txn->GetTransactionId()) {
		delete request;
		auto old_iter = iter;
		iter++;
		row_lock_map_[rid]->request_queue_.erase(old_iter);
		continue;;
	}

	// 如果这个事务还没被授予锁，则直接跳过
    if (!(*iter)->granted_) {
	  iter++;
      continue;
    }
	// 如果锁请求队列中已经有了这个事务的请求，则说明下面即将做的必然是锁升级！ 所以把这个锁删掉，然后重新请求一个升级锁
	if ((*iter) != request && (*iter)->txn_id_ == request->txn_id_) {
		CutDownRowLockModeCount(txn, (*iter)->lock_mode_, oid, rid);
		auto old_iter = iter;
		iter++;
		delete *old_iter;
		row_lock_map_[rid]->request_queue_.erase(old_iter);
		continue;
	}
	// 如果这个事务的锁不兼容
    if (!IsLockCompatible((*iter)->lock_mode_, request->lock_mode_) && (*iter)->txn_id_!=request->txn_id_) {
      row_lock_map_latch_.unlock();
	  row_lock_map_[rid]->wait(txn);
	  row_lock_map_latch_.lock();
      iter = row_lock_map_[rid]->request_queue_.begin();
    }else {
		iter++;
	}
  }

  if (txn->GetState()==TransactionState::ABORTED) {
	row_lock_map_latch_.unlock();
	return false;
  }

  // 判断一下是否可以升级，不能升级的话another_request必为空
  txn->SetState(TransactionState::GROWING);
  request->granted_ = true;
  row_lock_map_latch_.unlock();
  // 更新事务所持有锁的记录
  AddLockRowModeCount(txn, lock_mode, oid, rid);
  return true;

}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool { 
  // 1. 先判断表是否已经有了请求队列
  row_lock_map_latch_.lock();
//   // 2. 放弃意向锁
//   UnlockTable(txn, oid);
  // 3. 必须有对该行的锁
  if (row_lock_map_.count(rid) == 0) {
    return false;
  }
  // 
  for (auto iter = row_lock_map_[rid]->request_queue_.begin(); iter != row_lock_map_[rid]->request_queue_.end();
       iter++) {
    if ( (*iter)->granted_ && (*iter)->txn_id_==txn->GetTransactionId()) {
	  CutDownRowLockModeCount(txn, (*iter)->lock_mode_, oid, rid);
	  delete *iter;
	  row_lock_map_[rid]->request_queue_.erase(iter);
      row_lock_map_[rid]->notify_one();
	  row_lock_map_latch_.unlock();
	  if (txn->GetState()==TransactionState::GROWING) {
	  	txn->SetState(TransactionState::SHRINKING);
	  }
      return true;
    }
  }
  // 如果没有找到用户的请求...
  row_lock_map_latch_.unlock();
  return false;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
	// 1. 构建一条边
	std::pair<txn_id_t, txn_id_t> edge(t1, t2);
	// 2. 加入到map中
	if (edges_map_.count(t1)==0) {
		edges_map_.insert({t1, std::vector<std::pair<txn_id_t, txn_id_t>>{edge}});
	}else {
		edges_map_[t1].push_back(edge);
	}
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
	std::pair<txn_id_t, txn_id_t> edge(t1, t2);
	if (edges_map_.count(t1)==0) {
		return;
	}else {
		for (auto it = edges_map_[t1].begin(); it < edges_map_[t1].end(); it++) {
			if ((*it).first == t1 && (*it).second == t2) {
				edges_map_[t1].erase(it);
				return;
			}
		}
	}
}

auto LockManager::HasCycle(txn_id_t *result) -> bool { 
	std::unordered_set<txn_id_t> visited_id;
	auto edge_list  = GetEdgeList();
	if (edge_list.empty()) {
		return false;
	}
	for (auto & edge: edge_list) {
		if (visited_id.count(edge.first) > 0) {
			continue;
		}
		if (HasCycle_(result, edge.first, visited_id)) {
			return true;
		}
	}
	return false;
}

auto LockManager::HasCycle_(txn_id_t *result, txn_id_t txn_id, std::unordered_set<txn_id_t>& visited_id) -> bool {
	visited_id.insert(txn_id);
	for (auto iter = edges_map_[txn_id].begin(); iter != edges_map_[txn_id].end(); iter++) {
		if (visited_id.count(iter->second) > 0) {
			if (result!=nullptr)
				*result = txn_id;
			return true;
		}else {
			if (HasCycle_(result, iter->second, visited_id)) {
				return true;
			}
		}
	}
	visited_id.erase(txn_id);
	return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for (auto iter = edges_map_.begin(); iter!= edges_map_.end(); iter++) {
		for (auto edge: iter->second) {
			edges.push_back(edge);
		}
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
	// 如何构建一个等待图？
	table_lock_map_latch_.lock();
	{
		std::unordered_map<table_oid_t, std::vector<std::pair<LockMode, txn_id_t>>> hold_map;
		std::unordered_map<table_oid_t, std::vector<std::pair<LockMode, txn_id_t>>> wait_map;
		for (auto it = table_lock_map_.begin(); it!=table_lock_map_.end(); it++) {
			for (LockRequest * req: it->second->request_queue_) {
				// 说明是被等待者
				if ( req->granted_ ) {
					if (hold_map.count(req->oid_)==0) {
						hold_map[req->oid_]=std::vector<std::pair<LockMode, txn_id_t>>();
					}
					hold_map[req->oid_].push_back({req->lock_mode_, req->txn_id_});
				}
				// 说明是等待者
				else {
					if (wait_map.count(req->oid_)==0) {
						hold_map[req->oid_].push_back({req->lock_mode_, req->txn_id_});
					}
					wait_map[req->oid_].push_back({req->lock_mode_, req->txn_id_});
				}
			}
		}
		// 遍历等待者集合
		for (auto it = wait_map.begin(); it!=wait_map.end(); it++) {
			// 获取等待者的ID
			table_oid_t wait_table = it->first;
			// 看看是否有持有者
			if (hold_map.count(wait_table) == 0) {
				continue;;
			}
			// 获取这个表上的所有持有者
			auto hold_set = hold_map[wait_table];
			// 然后遍历这个表上的锁
			for (std::pair<LockMode, txn_id_t> wait_pair: it->second) {
				// 找到对应的持有者，加入到边种
				for (std::pair<LockMode, txn_id_t>  hold_pair: hold_set) {
					// 如果不兼容的话，则说明存在等待关系
					if (!IsLockCompatible(wait_pair.first, hold_pair.first)) {
						AddEdge(wait_pair.second, hold_pair.second);
					}
				}
			}
		}
	}
	table_lock_map_latch_.unlock();

	row_lock_map_latch_.lock();
	// 构建row lock的等待图
	{
		std::unordered_map<RID, std::vector<std::pair<LockMode, txn_id_t>>> hold_map_row;
		std::unordered_map<RID, std::vector<std::pair<LockMode, txn_id_t>>> wait_map_row;
		for (auto it = row_lock_map_.begin(); it!=row_lock_map_.end(); it++) {
			for (LockRequest * req: it->second->request_queue_) {
				// 说明是被等待者
				if ( req->granted_ ) {
					if (hold_map_row.count(req->rid_)==0) {
						hold_map_row[req->rid_] = std::vector<std::pair<LockMode, txn_id_t>>();;
					}
					hold_map_row[req->rid_].push_back({req->lock_mode_, req->txn_id_});
				}
				// 说明是等待者
				else {
					if (wait_map_row.count(req->rid_)==0) {
						wait_map_row[req->rid_]= std::vector<std::pair<LockMode, txn_id_t>>();
					}
					wait_map_row[req->rid_].push_back({req->lock_mode_, req->txn_id_});
				}
			}
		}
		for (auto it = wait_map_row.begin(); it!=wait_map_row.end(); it++) {
			RID wait_rid = it->first;
			if (wait_map_row.count(wait_rid) == 0) {
				continue;;
			}
			auto hold_set = hold_map_row[wait_rid];
			for (std::pair<LockMode, txn_id_t> wait_pair: it->second) {
				for (std::pair<LockMode, txn_id_t>  hold_pair: hold_set) {
					// 如果不兼容的话，则说明存在等待关系
					if (!IsLockCompatible(wait_pair.first, hold_pair.first)) {
						AddEdge(wait_pair.second, hold_pair.second);
					}
				}
			}
		}

	}
	row_lock_map_latch_.unlock();
	// 开始计算是否出现环
    {
		txn_id_t cycle_id;
		bool is_cycle = HasCycle(&cycle_id);
		if (!is_cycle) {
			edges_map_.clear();
			continue;
		}
		std::cout<<"aboart\n";
		// 直接将这个事务终止掉
		Transaction * cicyle_transaction = TransactionManager::GetTransaction(cycle_id);
		TransactionManager txn_mgr{this};
		txn_mgr.Abort(cicyle_transaction);
		edges_map_.clear();
    }
  }
}

void LockManager::LockRequestQueue::wait(Transaction * txn) {
  auto lock = std::unique_lock(this->latch_);
  this->cv_.wait(lock);
}

void LockManager::LockRequestQueue::notify_one() { this->cv_.notify_one(); }

bool LockManager::IsLockCompatible(LockMode lock1, LockMode lock2) {
  if (this->compatible_map[lock1].count(lock2) > 0) {
    return true;
  }
  return false;
}

}
 