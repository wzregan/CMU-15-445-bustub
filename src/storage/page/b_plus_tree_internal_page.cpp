//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::INTERNAL_PAGE);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return children_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  children_[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::BinarySearch(const KeyType & key, int *idx, KeyComparator cmp) const -> bool{
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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SearchValueByKey(const KeyType & key, ValueType *value, KeyComparator cmp) const -> bool {
  int idx;
  bool success = BinarySearch(key, &idx, cmp);
  if (!success) {
    return false;
  }
  *value = ValueAt(idx);
  return true;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return children_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Delete(const KeyType & key, ValueType *value,  KeyComparator cmp) -> bool {
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
    *value = children_[idx].second;
  }
  for (int i = idx; i < current_size - 1; i++) {
    children_[i] = children_[i+1];
  }

  IncreaseSize(-1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType & key, const ValueType & value,  KeyComparator cmp) -> bool {
  int current_size = GetSize();

  int idx;

  bool exist = BinarySearch(key, &idx, cmp);
  if (exist) {
    return false;
  }
  // 向后拷贝
  for (int i = current_size; i > idx ; i--) {
    children_[i] = children_[i - 1];
  }
  // 给孩子赋值
  children_[idx].first  = key;
  children_[idx].second = value;
  // 添加大小
  IncreaseSize(1);
  return true;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<int, int, DefaultComparator<int>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
