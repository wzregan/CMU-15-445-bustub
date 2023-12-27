//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/index/b_plus_tree.h"
namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetMaxSize(max_size);
  SetParentPageId(parent_id);
  SetPageId(page_id);
  SetSize(0);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetNextPageId(INVALID_PAGE_ID);
  SetPrePageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  this->next_page_id_ = next_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetPrePageId() const -> page_id_t { return pre_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetPrePageId(page_id_t pre_page_id) {
  this->pre_page_id_ = pre_page_id;
}


/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}


// helper
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::BinarySearch(const KeyType & key, int *idx, KeyComparator cmp) const -> bool{
  // 根据给定的key，找到下一个节点
  int L = 0;
  int R = GetSize();
  int mid;
  int cmp_ret;
  while ( L < R) {
    mid = (L + R) / 2;
    cmp_ret = cmp(KeyAt(mid), key);
    if ( cmp_ret > 0 ) {
      R = mid;
    }else if( cmp_ret < 0 ) {
      L = mid + 1;
    }else{
      if (idx!=nullptr) {
        *idx = mid;
      }
      return true;
    }
  }
  if (idx!=nullptr) {
    *idx = L;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::SetValue(int index, const ValueType & value) -> void{
  array_[index].second = value;
}
// helper，插入帮助函数，一定可以插入成功，但是插入之后如果满了，则返回true
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertRecard(const KeyType & key, const ValueType & value, KeyComparator & cmp) -> bool {
  int idx;
  bool findable = BinarySearch(key, &idx, cmp);
  if (findable) {
    // 如果找到了，则更新值
    SetValue(idx, value);
    return false;
  }else{
    // 如果没有找到，则插入
    // 1. 向后拷贝
    int current_size = GetSize();

    for (int i = current_size; i > idx ; i--) {
      array_[i] = array_[i - 1];
    }
    array_[idx].first = key;
    array_[idx].second = value;
    IncreaseSize(1);
    // 判断是否满了
    return GetMaxSize() < current_size + 1;
  }
}

// helper，删除帮助函数，返回true则说明删除成功
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::DeleteRecard(const KeyType & key, ValueType * value, KeyComparator & cmp) -> bool {
  // 删除记录
  int current_size = GetSize();
  if (current_size == 0) {
    return false;
  }
  int idx;
  bool isfound = BinarySearch(key, &idx, cmp);
  if (!isfound) {
    return false;
  }
  if (value!=nullptr) {
    *value = array_[idx].second;
  }
  for (int i = idx; i < current_size - 1; i++) {
    array_[i] = array_[i+1];
  }
  IncreaseSize(-1);
  return true;
}



INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Get(const KeyType & key, KeyComparator & cmp, std::vector<ValueType> *result) -> void{
  int idx;
  bool findable = BinarySearch(key, &idx, cmp);
  if (findable) {
    result->emplace_back(array_[idx].second);
  }
}
template class BPlusTreeLeafPage<int, int, DefaultComparator<int>>;
template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
