//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "container/hash/hash_function.h"
#include "storage/page/page.h"
namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(1), bucket_size_(bucket_size), num_buckets_(1), dir_(bucket_size, std::shared_ptr<Bucket>()) {
  num_buckets_ = pow(bucket_size_, 1);
  for (int i = 0; i < pow(bucket_size_, 1); i++) {
    dir_[i].reset(new Bucket(bucket_size_, 1, i));
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::recursive_mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::recursive_mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  uint64_t dir_idx = IndexOf(key);
  if (!dir_[dir_idx]) {
    return false;
  } 
  return dir_[dir_idx]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  latch_.lock();
  uint64_t dir_idx = IndexOf(key);
  bool ret = dir_[dir_idx]->Remove(key);
  latch_.unlock();
  return ret;
}

/*
TODO
1. 必须是可重入锁吗？
2. 需要读写锁吗？
*/
template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  latch_.lock();
  uint64_t dir_idx = IndexOf(key);
  bool extend_flag = dir_[dir_idx]->IsFull();
  dir_[dir_idx]->Insert(key, value);
  if (!extend_flag) {  // 判断有没有满，如果满的话，那么我们就需要对bucket进行分裂，以及增加深度
    latch_.unlock();
    return;
  }
  int dir_idx_depth = dir_[dir_idx]->IncrementDepth();  // 满了，则bucket的深度+1

  if (dir_idx_depth > GetGlobalDepth()) {
    // 则需要对其进行扩展
    ExtendDirectory();
  }

  // 对Bucket进行分裂，并且获取原来的bucket
  std::shared_ptr<Bucket> old_bucket = SplitBucket(dir_idx);

  // 对旧的的Bucket进行Rehash
  std::list<std::pair<K, V>> &old_items = old_bucket->GetItems();

  for (std::pair<K, V> &item : old_items) {
    Insert(item.first, item.second);
  }
  latch_.unlock();
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Contained(const K &key) ->bool {
  for (auto iter = list_.begin(); iter != list_.end(); iter++) {
    if ((*iter).first == key) {
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth, int mdi)
    : min_diridx_(mdi), size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::SplitBucket(int dir_idx) -> std::shared_ptr<Bucket> {
  if (GetLocalDepth(dir_idx) < GetGlobalDepth()) {
    return std::shared_ptr<Bucket>();
  }
  // 原来old的bucket
  std::shared_ptr<Bucket> old_bucket = dir_[dir_idx];
  int pointed_min_dir_idx = dir_[dir_idx]->GetMinDirIdx();
  // 计算有几个指针指向dir_
  int referenced_pointed_count = static_cast<int>(pow(2, GetGlobalDepth() - GetLocalDepth(dir_idx) + 1));
  //  修改指向old_bucket的指针
  int current_modify_pointer = pointed_min_dir_idx;

  std::shared_ptr<Bucket> new_temp1 = std::make_shared<Bucket>(2, GetGlobalDepth(), pointed_min_dir_idx);
  std::shared_ptr<Bucket> new_temp2 =
      std::make_shared<Bucket>(2, GetGlobalDepth(), pointed_min_dir_idx + (referenced_pointed_count - 1) * 4);
  int old_dir_size = static_cast<int>(pow(2, GetGlobalDepth() - 1));
  for (int c = 0; c < referenced_pointed_count; c++) {
    if (c < referenced_pointed_count / 2) {
      dir_[current_modify_pointer] = new_temp1;
    } else {
      dir_[current_modify_pointer] = new_temp2;
    }
    current_modify_pointer += old_dir_size;
  }
  num_buckets_++;
  return old_bucket;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::ExtendDirectory() -> int {
  size_t start = dir_.size(); 
  size_t copy_cursor = 0;

  dir_.resize(dir_.size() * 2, std::shared_ptr<Bucket>());
  for (; start < dir_.size(); start++) {
    dir_[start] = dir_[copy_cursor++];
  }
  global_depth_++;
  return global_depth_;
}
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (auto iter = list_.begin(); iter != list_.end(); iter++) {
    if ((*iter).first == key) {
      value = (*iter).second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto iter = this->list_.begin(); iter != this->list_.end(); iter++) {
    if ((*iter).first == key) {
      this->list_.erase(iter);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for (auto iter = this->list_.begin(); iter != this->list_.end(); iter++) {
    if ((*iter).first == key) {
      (*iter).second = value;
      return true;
    }
  }
  auto kv_pair = std::make_pair(key, value);
  this->list_.push_back(kv_pair);
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::IncreaseDepth() -> int {
  this->depth_++;
  return this->depth_;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;
template class ExtendibleHashTable<std::string, int>;
template class ExtendibleHashTable<const char *, int>;

}  // namespace bustub
