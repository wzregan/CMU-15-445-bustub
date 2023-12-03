/**
 * extendible_hash_test.cpp
 */

#include "container/hash/extendible_hash_table.h"
#include <memory>
#include <string>
#include <thread>  // NOLINT
#include "gtest/gtest.h"
using std::string;
namespace bustub {
TEST(ExtendibleHashTableTest, ENABLED_SampleTest3) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(4);
  table->Insert(4, "a");  
  table->Insert(12, "a");
  table->Insert(16, "a");
  table->Insert(64, "a");
  table->Insert(5, "a");
  table->Insert(10, "a");
  table->Insert(51, "a");
  table->Insert(15, "a");
  table->Insert(18, "a");
  table->Insert(20, "a");
  table->Insert(7, "a");

  table->Insert(21, "a");
  table->Insert(11, "a");
  table->Insert(19, "a");
  std::cout<<table->GetLocalDepth(5)<<"\n";

  EXPECT_EQ(3, table->GetLocalDepth(3));

}

TEST(ExtendibleHashTableTest, ENABLED_SampleTest2) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(4);
  table->Insert(1, "a");
  table->Insert(2, "b");
  table->Insert(3, "c");
  table->Insert(4, "d");
  EXPECT_EQ(table->GetGlobalDepth(), 0);
  table->Insert(5, "e");
  table->Insert(6, "f");
  table->Insert(7, "g");
  table->Insert(8, "h");
  EXPECT_EQ(table->GetGlobalDepth(), 1);
  table->Insert(9, "h");
  EXPECT_EQ(table->GetGlobalDepth(), 2);
  EXPECT_EQ(table->GetNumBuckets(), 3);
  EXPECT_EQ(table->dir_[0], table->dir_[2]);

  EXPECT_EQ(table->dir_[1]->GetItems().size(), 3);
  EXPECT_EQ(table->dir_[3]->GetItems().size(), 2);

  table->Insert(12, "m");
  EXPECT_EQ(table->GetLocalDepth(0), 2);
  EXPECT_EQ(table->dir_[2]->GetItems().size(), 2);
  table->Insert(16, "m");
  table->Insert(20, "m");
  EXPECT_EQ(table->GetGlobalDepth(), 3);
  EXPECT_EQ(table->dir_[0]->GetItems().size(), 2);
  string v;
  
  EXPECT_EQ(table->Find(10000,v),false);

}
TEST(ExtendibleHashTableTest,ENABLED_SampleTest) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(2);
  EXPECT_EQ(0, table->GetGlobalDepth());
  EXPECT_EQ(0, table->GetLocalDepth(0));

  table->Insert(1, "a");
  table->Insert(2, "b");
  table->Insert(3, "c");
  table->Insert(4, "d");
  table->Insert(5, "e");
  table->Insert(6, "f");
  table->Insert(7, "g");
  table->Insert(8, "h");
  EXPECT_EQ(2, table->GetGlobalDepth());
  EXPECT_EQ(2, table->GetLocalDepth(0));
  EXPECT_EQ(2, table->GetLocalDepth(1));
  EXPECT_EQ(2, table->GetLocalDepth(2));
  EXPECT_EQ(2, table->GetLocalDepth(3));

  table->Insert(9, "i");
  EXPECT_EQ(3, table->GetGlobalDepth());

  EXPECT_EQ(2, table->GetLocalDepth(0));
  EXPECT_EQ(3, table->GetLocalDepth(1));
  EXPECT_EQ(2, table->GetLocalDepth(2));
  EXPECT_EQ(2, table->GetLocalDepth(3));
  EXPECT_EQ(3, table->GetLocalDepth(5));


  std::string result;
  table->Find(9, result);
  EXPECT_EQ("i", result);
  table->Find(8, result);
  EXPECT_EQ("h", result);
  table->Find(2, result);
  EXPECT_EQ("b", result);
  EXPECT_FALSE(table->Find(10, result));

  EXPECT_TRUE(table->Remove(8));
  EXPECT_TRUE(table->Remove(4));
  EXPECT_TRUE(table->Remove(1));
  EXPECT_FALSE(table->Remove(20));

  auto table2 = std::make_unique<ExtendibleHashTable<int, int>>(2);
  int result2 = 0;
  for (int i = 0; i < 1000;i++)
  {
    table2->Insert(i, i*i);
    table2->Find(i, result2);
    EXPECT_EQ(i *i, result2);
  }
  for (int i = 0; i < 1000;i++)
  {
    
    EXPECT_EQ(table2->Remove(i), true);
  }
  for (int i = 0; i < 1000;i++)
  {
    EXPECT_EQ(false, table2->Find(i, result2));
  }
    for (int i = 0; i < 1000;i++)
  {
    table2->Insert(i, i*i);
    table2->Find(i, result2);
    EXPECT_EQ(i *i, result2);
  }


}

TEST(ExtendibleHashTableTest, ENABLED_ConcurrentInsertTest) {
  const int num_runs = 50;
  const int num_threads = 3;

  // Run concurrent test multiple times to guarantee correctness.
  for (int run = 0; run < num_runs; run++) {
    auto table = std::make_unique<ExtendibleHashTable<int, int>>(2);
    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int tid = 0; tid < num_threads; tid++) {
      threads.emplace_back([tid, &table]() { table->Insert(tid, tid); });
    }
    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    EXPECT_EQ(table->GetGlobalDepth(), 1);
    for (int i = 0; i < num_threads; i++) {
      int val;
      EXPECT_TRUE(table->Find(i, val));
      EXPECT_EQ(i, val);
    }
  }
}

}  // namespace bustub
TEST(HASH_TABLE_BUCKET_TEST, ENABLED_INSERT_TEST) {
  bustub::ExtendibleHashTable<string, int>::Bucket bucket(2, 1);
  EXPECT_EQ(bucket.GetCapacity(), 2);
}
