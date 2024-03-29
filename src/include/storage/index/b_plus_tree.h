//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <string>
#include <vector>

#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

template<class ValueType>
class DefaultComparator{
public:
  int operator()(const ValueType & a, const ValueType & b ){
    return a - b;
  }
};


#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */



INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr) -> bool;

  // return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  // 辅助函数，给定key，找出该key所在的leafpage和idx
  auto LocatePage(const KeyType &key, int *idx) -> LeafPage*;

  template<class PageNode>
  auto Merge(PageNode * mergein, const KeyType & deleted_key) -> InternalPage*;

  template<class PageNode>
  auto Merge(PageNode * merge_b, PageNode * merge_s) -> void;
  // 如果返回不为空，则说明返回的这个节点也需要修复
  template<class PageNode>
  auto FixInternalNode(PageNode *pn, const KeyType & deleted_key) -> void;

  template<class PageNode>
  auto BorrowBrother(PageNode * node, bool left) -> bool;


  // index iterator
  auto Begin() -> INDEXITERATOR_TYPE;
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;
  auto End() -> INDEXITERATOR_TYPE;

  // print the B+ tree
  void Print(BufferPoolManager *bpm);

  // draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  auto Search(const KeyType &key, std::vector<ValueType> *result) -> void;
  
  template<class PageNode>
  void UpdateNode(PageNode *node);
  
  int fetch_count{0};
  int unpin_count{0};

 private:
  // search
  // split
  template<class T>
  auto SplitNode(T * origin, T * splited) -> void;
  auto SplitLeafPageNode(LeafPage *leaf) -> InternalPage *;

  auto SplitInternalPageNode(InternalPage * spliting_node) -> InternalPage *;

  auto UpdateChildrenParent(InternalPage * parent) -> void;

  template<class T>
  auto UnpinPageNode(T * node, bool is_dirty) -> void;

  template<class T>
  auto FetchPageNode(page_id_t page_id) -> T*;

  template<class T>
  auto NewPageNode(page_id_t parent_page_id, int max_size) -> T*;

  template<class PageNode>
  auto SplitPageNode(PageNode *leaf_node) -> InternalPage *;

  void UpdateRootPageId(int insert_record = 0);

  template <class T>
  auto ToInternalPage(T *page_data) -> InternalPage *;

  template <class T>
  auto ToGeneralPage(T *page_data) -> BPlusTreePage *;

  template <class T>
  auto ToLeafPage(T *page_data) -> LeafPage *;

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  // member variable
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
};

}  // namespace bustub
