/**
 * lru_k_replacer_test.cpp
 */

#include "buffer/lru_k_replacer.h"

#include <algorithm>
#include <cstdio>
#include <memory>
#include <random>
#include <set>
#include <thread>  // NOLINT
#include <vector>

#include "gtest/gtest.h"

namespace bustub {

TEST(LRUKReplacerTest, DISABLED_SampleTest) {
  LRUKReplacer lru_replacer(7, 2);

  // Scenario: add six elements to the replacer. We have [1,2,3,4,5]. Frame 6 is non-evictable.
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(2);
  lru_replacer.RecordAccess(3);
  lru_replacer.RecordAccess(4);
  lru_replacer.RecordAccess(5);
  lru_replacer.RecordAccess(6);
  lru_replacer.SetEvictable(2, true);
  lru_replacer.SetEvictable(3, true);
  lru_replacer.SetEvictable(4, true);
  lru_replacer.SetEvictable(5, true);
  lru_replacer.SetEvictable(6, false);
  ASSERT_EQ(5, lru_replacer.Size());  // 1 2 3 4 5 // 6

  // Scenario: Insert access history for frame 1. Now frame 1 has two access histories.
  // All other frames have max backward k-dist. The order of eviction is [2,3,4,5,1].
  lru_replacer.RecordAccess(1);

  // Scenario: Evict three pages from the replacer. Elements with max k-distance should be popped
  // first based on LRU.
  int value;
  lru_replacer.Evict(&value);
  ASSERT_EQ(2, value);
  lru_replacer.Evict(&value);
  ASSERT_EQ(3, value);
  lru_replacer.Evict(&value);
  ASSERT_EQ(4, value);
  ASSERT_EQ(2, lru_replacer.Size());

  // Scenario: Now replacer has frames [5,1].
  // Insert new frames 3, 4, and update access history for 5. We should end with [3,1,5,4]
  lru_replacer.RecordAccess(3);
  lru_replacer.RecordAccess(4);
  lru_replacer.RecordAccess(5);
  lru_replacer.RecordAccess(4);
  lru_replacer.SetEvictable(3, true);
  lru_replacer.SetEvictable(4, true);
  ASSERT_EQ(4, lru_replacer.Size());

  // Scenario: continue looking for victims. We expect 3 to be evicted next.
  lru_replacer.Evict(&value);
  ASSERT_EQ(3, value);
  ASSERT_EQ(3, lru_replacer.Size());

  // Set 6 to be evictable. 6 Should be evicted next since it has max backward k-dist.
  lru_replacer.SetEvictable(6, true);
  ASSERT_EQ(4, lru_replacer.Size());
  lru_replacer.Evict(&value);
  ASSERT_EQ(6, value);
  ASSERT_EQ(3, lru_replacer.Size());

  // Now we have [1,5,4]. Continue looking for victims.
  lru_replacer.SetEvictable(1, false);
  ASSERT_EQ(2, lru_replacer.Size());
  ASSERT_EQ(true, lru_replacer.Evict(&value));
  ASSERT_EQ(5, value);
  ASSERT_EQ(1, lru_replacer.Size());

  // Update access history for 1. Now we have [4,1]. Next victim is 4.
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(1);
  lru_replacer.SetEvictable(1, true);
  ASSERT_EQ(2, lru_replacer.Size());
  ASSERT_EQ(true, lru_replacer.Evict(&value));
  ASSERT_EQ(value, 4);

  ASSERT_EQ(1, lru_replacer.Size());
  lru_replacer.Evict(&value);
  ASSERT_EQ(value, 1);
  ASSERT_EQ(0, lru_replacer.Size());

  // These operations should not modify size
  ASSERT_EQ(false, lru_replacer.Evict(&value));
  ASSERT_EQ(0, lru_replacer.Size());
  lru_replacer.Remove(1);
  ASSERT_EQ(0, lru_replacer.Size());
}

TEST(DoubleLinkedListt, ENABLE_DoubleLinkedListSampleTest) {
  DoubleLinkedList<frame_id_t> list;
  list.InsertFront(1);
  list.InsertFront(2);
  list.InsertFront(3);
  list.InsertFront(4);
  list.InsertTail(0);
  EXPECT_EQ(list.Size(), 5);
  auto temp = list.head_->next_;
  for (int i = 1; i <= 5; i++) {
    EXPECT_EQ(temp->value_, 5 - i);
    temp = temp->next_;
  }
  frame_id_t id;
  list.RemoveTail(&id);
  EXPECT_EQ(id, 0);

  DoubleLinkedList<frame_id_t> list2;
  DoubleLinkedList<frame_id_t>::Node *n1 = new DoubleLinkedList<frame_id_t>::Node(1, 4);
  DoubleLinkedList<frame_id_t>::Node *n11 = new DoubleLinkedList<frame_id_t>::Node(5, 4);
  DoubleLinkedList<frame_id_t>::Node *n2 = new DoubleLinkedList<frame_id_t>::Node(2, 3);
  DoubleLinkedList<frame_id_t>::Node *n3 = new DoubleLinkedList<frame_id_t>::Node(3, 2);
  list2.InsertOrdered(n1);
  list2.InsertOrdered(n2);
  list2.InsertOrdered(n3);
  list2.InsertOrdered(n11);  //  n3 n2 n1 n11
  EXPECT_EQ(n1->next_, n11);
  EXPECT_EQ(n1, n11->pre_);
  EXPECT_EQ(n3->evictable_, true);

  temp = list2.head_->next_;
  for (int i = 1; i < 4; i++) {
    std::cout << temp->value_ << "\n";
    EXPECT_EQ(temp->value_, 4 - i);
    temp = temp->next_;
  }
  auto en1 = list2.FindFirstEvictableNode();
  EXPECT_EQ(en1, n3);
  n3->evictable_ = false;
  en1 = list2.FindFirstEvictableNode();
  EXPECT_EQ(en1, n2);
  n2->evictable_ = false;
  en1 = list2.FindFirstEvictableNode();
  EXPECT_EQ(en1, n1);

  n1->evictable_ = false;
  en1 = list2.FindFirstEvictableNode();
  EXPECT_EQ(en1, n11);

  n11->evictable_ = false;
  en1 = list2.FindFirstEvictableNode();
  EXPECT_EQ(en1, nullptr);



}

TEST(LRU_K, ENABLE_LRU_K_SampleTest) {
  LruK lruk(8, 3);// L:  H:
  lruk.Access(1);// L:1  H:
  lruk.Access(2);// L:1 2  H:
  lruk.Access(3);// L:1 2 3  H:
  lruk.Access(3);// L:1 2 3 H:
  lruk.Access(3);// L:1 2 H: 3
  lruk.Access(3);// L:1 2 H: 3

  lruk.Access(1);// L:1 2 H: 3
  lruk.Access(2);// L:1 2 H: 3

  frame_id_t id;
  bool evict_flag = lruk.Evict(&id);
  EXPECT_EQ(evict_flag, true);
  EXPECT_EQ(id, 1);

  lruk.Access(4);// L:2 4 H: 3
  lruk.Access(2);// L:2 4 H: 3

  evict_flag = lruk.Evict(&id);
  EXPECT_EQ(evict_flag, true);
  EXPECT_EQ(id, 4);

  evict_flag = lruk.Evict(&id);
  EXPECT_EQ(evict_flag, true);
  EXPECT_EQ(id, 3);
  evict_flag = lruk.Evict(&id);
  EXPECT_EQ(evict_flag, true);
  EXPECT_EQ(id, 2);
  EXPECT_EQ(lruk.Size(), 0);

  lruk.Access(1);
  lruk.Access(2);
  lruk.Access(3);
  lruk.Access(4);
  lruk.SetEvictable(1, false);
  evict_flag = lruk.Evict(&id);
  EXPECT_EQ(evict_flag, true);
  EXPECT_EQ(id, 2);
  EXPECT_EQ(lruk.Size(), 3);
  evict_flag = lruk.Evict(&id);
  EXPECT_EQ(evict_flag, true);
  EXPECT_EQ(id, 3);
  evict_flag = lruk.Evict(&id);
  EXPECT_EQ(evict_flag, true);
  EXPECT_EQ(id, 4);
  EXPECT_EQ(lruk.Size(), 1);

  DoubleLinkedList<frame_id_t> list3;
  for (int i = 0; i < 1000; i++){
    auto n = new DoubleLinkedList<frame_id_t>::Node(i);
    list3.InsertTailNode(n);
  }
  auto temp_list3 = list3.head_->next_;
  for (int i = 0; i < 1000; i++){
    EXPECT_EQ(temp_list3->value_, i);
    temp_list3 = temp_list3->next_;
  }
}

TEST(LRU_K, DISABLED_LRU_K_SampleTest2) {
  LruK lruk(4, 2);
  lruk.Access(1);// h:1 L:
  lruk.Access(2);// h:1 2 L:
  lruk.Access(1);// h:2 L:1
  lruk.Access(3);// h:2 3 L:1
  lruk.Access(3);// h:2 L:3 1

  frame_id_t id;
  bool evict_flag = lruk.Evict(&id);
  EXPECT_EQ(evict_flag, true);
  EXPECT_EQ(id, 2);
  
  lruk.Access(4);// h:4 L:3 1
  lruk.Access(5);// h:4 5 L:3 1

  evict_flag = lruk.Evict(&id);
  EXPECT_EQ(evict_flag, true);
  EXPECT_EQ(id, 4);// h:5 L:3 1

  evict_flag = lruk.Evict(&id);
  EXPECT_EQ(evict_flag, true);
  EXPECT_EQ(id, 5);// h: L:3 1

  evict_flag = lruk.Evict(&id);
  EXPECT_EQ(evict_flag, true);
  EXPECT_EQ(id, 1);// h: L:3 1



  evict_flag = lruk.Evict(&id);
  EXPECT_EQ(evict_flag, true);
  EXPECT_EQ(id, 3);// h: L:3 1

  evict_flag = lruk.Evict(&id);
  EXPECT_EQ(evict_flag, false);

}

TEST(LRU_K, DISABLED_LRU_K_SampleTest3) {
  LruK lruk(4, 3);
  lruk.Access(1);// h:1 L:
  lruk.Access(2);// h:1 2 L:
  lruk.Access(1);// h:1 2 L:
  lruk.Access(3);// h:1 2 3 L:
  frame_id_t id;
  bool evict_flag = lruk.Evict(&id);
  EXPECT_EQ(evict_flag, true);
  EXPECT_EQ(id, 1);
}

TEST(LRU_K, ENABLE_LRU_K_SampleTest4) {
  LRUKReplacer lruk1(10, 2);
  lruk1.RecordAccess(1);
  LRUKReplacer lruk2(10, 2);
  lruk2.RecordAccess(1);
  lruk2.SetEvictable(1,false);
  lruk2.RecordAccess(2);
  lruk2.Remove(2);
  lruk2.RecordAccess(3);
// 20: size:10 k:2
// 20: Insert:1,0
// 20: size:10 k:2
// 20: Insert:1,0
// 20: Insert:2,1
// 20: Insert:3,2
// 20: size:10 k:2
// 20: Insert:1,0
// 20: Insert:2,1
// 20: Insert:3,2
// 20: size:10 k:2
// 20: Insert:1,0
// 20: Insert:2,1
// 20: Insert:3,2
// 20: Insert:4,3
// 20: Insert:1,4
// 20: Insert:2,4
// 20: Insert:3,4
// 20: Insert:4,4
// 20: Insert:1,4
// 20: Insert:2,4
// 20: Insert:3,4
// 20: Insert:4,4
// 20: Insert:1,4
// 20: Insert:2,4
// 20: Insert:3,4
// 20: Insert:4,4
// 20: Insert:1,4
// 20: Insert:2,4
// 20: Insert:3,4
// 20: Insert:4,4
// 20: Insert:1,4
// 20: Insert:2,4
// 20: Insert:3,4
// 20: Insert:4,4
// 20: Insert:4,4
// 20: size:10 k:2
// 20: Insert:1,0
// 20: Insert:2,1
// 20: Insert:3,2
// 20: size:10 k:3
// 20: Insert:1,0
// 20: Insert:1,1
// 20: Insert:2,1
// 20: Insert:1,2
// 20: Evict
// 20: Evict
// 20: size:10 k:2
// 20: Insert:1,0
// 20: Insert:2,1
// 20: Insert:3,2

}


}  // namespace bustub
