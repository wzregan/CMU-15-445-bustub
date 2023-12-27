//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_insert_test.cpp
//
// Identification: test/storage/b_plus_tree_insert_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <set>
#include "buffer/buffer_pool_manager_instance.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT
#include <random>
namespace bustub {
TEST(BPlusTreeTests, DISABLED_InsertPrepare) {
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  page_id_t page_id;
  Page* header_page = bpm->NewPage(&page_id);

  DefaultComparator<int> cmp;
  auto header_node = reinterpret_cast<BPlusTreeInternalPage<int, int,DefaultComparator<int>>*>(header_page->GetData());
  header_node->Init(page_id, -1, 900);

  int size = header_node->GetSize();
  EXPECT_EQ(size, 0);
  
  bool insertable = header_node->Insert(10,10,cmp);
  EXPECT_EQ(insertable, true);
  EXPECT_EQ(header_node->GetSize(), 1);

  insertable = header_node->Insert(9,9,cmp);
  EXPECT_EQ(insertable, true);
  EXPECT_EQ(header_node->GetSize(), 2);

  insertable = header_node->Insert(11, 11,cmp);
  EXPECT_EQ(insertable, true);
  EXPECT_EQ(header_node->GetSize(), 3);

  insertable = header_node->Insert(8, 8,cmp);
  EXPECT_EQ(insertable, true);
  EXPECT_EQ(header_node->GetSize(), 4);


  int v;
  bool findable = header_node->SearchValueByKey(1,&v,cmp);
  EXPECT_EQ(findable, false);

  findable = header_node->SearchValueByKey(9,&v,cmp);
  EXPECT_EQ(findable, true);
  EXPECT_EQ(v, 9);

  findable = header_node->SearchValueByKey(10,&v,cmp);
  EXPECT_EQ(findable, true);
  EXPECT_EQ(v, 10);

  findable = header_node->SearchValueByKey(11,&v,cmp);
  EXPECT_EQ(findable, true);
  EXPECT_EQ(v, 11);

  findable = header_node->SearchValueByKey(8,&v,cmp);
  EXPECT_EQ(findable, true);
  EXPECT_EQ(v, 8);
  int iddx;

  EXPECT_FALSE(header_node->BinarySearch(2, &iddx, cmp));
  EXPECT_EQ(iddx,0);

  EXPECT_TRUE(header_node->BinarySearch(8, &iddx, cmp));
  EXPECT_EQ(iddx,0);

  EXPECT_TRUE(header_node->BinarySearch(9, &iddx, cmp));
  EXPECT_EQ(iddx,1);

  EXPECT_TRUE(header_node->BinarySearch(10, &iddx, cmp));
  EXPECT_EQ(iddx,2);

  EXPECT_TRUE(header_node->BinarySearch(11, &iddx, cmp));
  EXPECT_EQ(iddx,3);

  EXPECT_FALSE(header_node->BinarySearch(12, &iddx, cmp));
  EXPECT_EQ(iddx,4);
  std::set<int> sset;
  sset.insert(8);
  sset.insert(9);
  sset.insert(10);
  sset.insert(11);
  for (int i = 0; i < 500; i++) {
    int num = rand() % 1000;
    sset.insert(num);
    insertable = header_node->Insert(num, num,cmp);
  }
  EXPECT_EQ(sset.size(), header_node->GetSize());
  int c = 0;
  for (int i : sset) {
    int idx;
    bool search_result = header_node->BinarySearch(i, &idx, cmp);
    EXPECT_EQ(idx,c);
    EXPECT_EQ(i, header_node->KeyAt(c++));
    EXPECT_TRUE(search_result);
  }
  bool search_result = header_node->BinarySearch(1000, nullptr, cmp);
  EXPECT_FALSE(search_result);

  for (int i : sset) {
    int idx;
    EXPECT_TRUE(header_node->BinarySearch(i, &idx, cmp));
    int value;
    EXPECT_TRUE(header_node->SearchValueByKey(i, &value, cmp));
    EXPECT_EQ(value, i);
    EXPECT_TRUE(header_node->Delete(i, &idx, cmp));
    EXPECT_EQ(idx, i);
    EXPECT_FALSE(header_node->BinarySearch(i, &idx, cmp));
  }


  delete disk_manager;
  
  delete bpm;
}

TEST(BPlusTreeTests, DISABLED_InsertPrepare2) {
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  page_id_t page_id;
  Page* header_page = bpm->NewPage(&page_id);
  DefaultComparator<int> cmp;
  auto header_node = reinterpret_cast<BPlusTreeLeafPage<int, int, DefaultComparator<int>>*>(header_page->GetData());
  bool findable;

  findable = header_node->InsertRecard(1,2,cmp);
  EXPECT_FALSE(findable);
  std::vector<int> vec;
  header_node->Get(1,cmp,&vec);
  EXPECT_EQ(vec[0], 2);
  header_node->InsertRecard(1,1,cmp);
  vec.clear();
  header_node->Get(1,cmp,&vec);
  EXPECT_EQ(vec[0], 1);
  header_node->InsertRecard(2,2,cmp);

  header_node->InsertRecard(3,3,cmp);
  header_node->InsertRecard(4,4,cmp);
  header_node->InsertRecard(0,0,cmp);
  header_node->InsertRecard(5,5,cmp);
  vec.clear();
  header_node->Get(3,cmp,&vec);
  header_node->Get(4,cmp,&vec);
  header_node->Get(5,cmp,&vec);
  header_node->Get(0,cmp,&vec);
  EXPECT_EQ(vec[0], 3);
  EXPECT_EQ(vec[1], 4);
  EXPECT_EQ(vec[2], 5);
  EXPECT_EQ(vec[3], 0);
  int value;
  
  EXPECT_EQ(header_node->GetSize(), 6);
  EXPECT_TRUE(header_node->DeleteRecard(0,&value, cmp));
  EXPECT_EQ(header_node->GetSize(), 5);
  EXPECT_EQ(value, 0);
  EXPECT_FALSE(header_node->DeleteRecard(0,&value, cmp));
  int idx;
  EXPECT_FALSE(header_node->BinarySearch(0,&idx, cmp));
  EXPECT_EQ(idx, 0);
  EXPECT_FALSE(header_node->BinarySearch(-1,&idx, cmp));
  EXPECT_EQ(idx, 0);
  EXPECT_TRUE(header_node->BinarySearch(2,&idx, cmp));
  EXPECT_EQ(idx, 1);

  EXPECT_TRUE(header_node->BinarySearch(1,&idx, cmp));
  EXPECT_EQ(idx, 0);


  EXPECT_FALSE(header_node->BinarySearch(99,&idx, cmp));
  EXPECT_EQ(idx, header_node->GetSize());

  delete disk_manager;
  delete bpm;

}


TEST(BPlusTreeTests, ENABLE_InsertPrepare3){
  srand(0);
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");

  BufferPoolManager *bpm = new BufferPoolManagerInstance(20, disk_manager);
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 4, 5);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

    // create and fetch header_page
  std::vector<int64_t> keys;
  for (int64_t i = 1; i < 100; i++)
    keys.push_back(rand() % 50);
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  } // 
  tree.Draw(bpm, "btree.txt");
  std::vector<RID> vec;
  for (auto key : keys) {
    index_key.SetFromInteger(key);
    vec.clear();
    tree.Search(index_key, &vec);
    EXPECT_EQ(vec.size(), 1);
  }
  
  std::cout << "<iter>" << std::endl;
  auto iter = tree.Begin();
  while (!iter.IsEnd()) {
    std::cout << (*iter).first.ToString() << std::endl;
    ++iter;
  }
  std::cout << "<iter>" << std::endl;
  index_key.SetFromInteger(15);
  iter = tree.Begin(index_key);
  while (!iter.IsEnd()) {
    std::cout << (*iter).first.ToString() << std::endl;
    ++iter;
  }

  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");

}


TEST(BPlusTreeTests, DISABLED_InsertTest1) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  ASSERT_EQ(page_id, HEADER_PAGE_ID);
  (void)header_page;

  int64_t key = 42;
  int64_t value = key & 0xFFFFFFFF;
  rid.Set(static_cast<int32_t>(key), value);
  index_key.SetFromInteger(key);
  tree.Insert(index_key, rid, transaction);

  auto root_page_id = tree.GetRootPageId();
  auto root_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id)->GetData());
  ASSERT_NE(root_page, nullptr);
  ASSERT_TRUE(root_page->IsLeafPage());

  auto root_as_leaf = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(root_page);
  ASSERT_EQ(root_as_leaf->GetSize(), 1);
  ASSERT_EQ(comparator(root_as_leaf->KeyAt(0), index_key), 0);

  bpm->UnpinPage(root_page_id, false);
  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeTests, DISABLED_InsertTest2) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t size = 0;
  bool is_present;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    EXPECT_EQ(is_present, true);
    EXPECT_EQ(rids.size(), 1);
    EXPECT_EQ(rids[0].GetPageId(), 0);
    EXPECT_EQ(rids[0].GetSlotNum(), key);
    size = size + 1;
  }

  EXPECT_EQ(size, keys.size());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeTests, DISABLED_InsertTest3) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  ASSERT_EQ(page_id, HEADER_PAGE_ID);
  (void)header_page;

  std::vector<int64_t> keys = {5, 4, 3, 2, 1};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  start_key = 3;
  current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }
  
  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}
}  // namespace bustub
