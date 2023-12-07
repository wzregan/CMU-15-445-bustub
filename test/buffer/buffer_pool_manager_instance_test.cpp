//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance_test.cpp
//
// Identification: test/buffer/buffer_pool_manager_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include <cstdio>
#include <random>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
// Check whether pages containing terminal characters can be recovered
TEST(BufferPoolManagerInstanceTest, ENABLE_BinaryDataTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 5;

  std::random_device r;
  std::default_random_engine rng(r());
  std::uniform_int_distribution<char> uniform_dist(0);

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManagerInstance(buffer_pool_size, disk_manager, k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  char random_binary_data[BUSTUB_PAGE_SIZE];
  // Generate random binary data
  for (char &i : random_binary_data) {
    i = uniform_dist(rng);
  }

  // Insert terminal characters both in the middle and at end
  random_binary_data[BUSTUB_PAGE_SIZE / 2] = '\0';
  random_binary_data[BUSTUB_PAGE_SIZE - 1] = '\0';

  // Scenario: Once we have a page, we should be able to read and write content.
  std::memcpy(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE);
  EXPECT_EQ(0, std::memcmp(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} we should be able to create 5 new pages
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(i, true));
    bpm->FlushPage(i);
  }
  for (int i = 0; i < 5; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
    bpm->UnpinPage(page_id_temp, false);
  }
  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm->FetchPage(0);
  EXPECT_EQ(0, memcmp(page0->GetData(), random_binary_data, BUSTUB_PAGE_SIZE));
  EXPECT_EQ(true, bpm->UnpinPage(0, true));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerInstanceTest, ENABLE_SampleTest4) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 1;
  const size_t k = 5;
  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManagerInstance(buffer_pool_size, disk_manager, k);
  page_id_t pgid;
  bpm->NewPage(&pgid);
  bpm->UnpinPage(pgid, true);
  bpm->FetchPage(pgid);
  bpm->UnpinPage(pgid, false);
  Page *p = bpm->FetchPage(pgid);
  EXPECT_EQ(p->IsDirty(), true);

  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerInstanceTest, ENABLE_SampleTest3) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 5;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManagerInstance(buffer_pool_size, disk_manager, k);
  page_id_t page_id[22];
  for (int i = 0; i < 10; i++) {
    bpm->NewPage(&page_id[i]);
  }
  for (int i = 0; i < 10; i++) {
    bpm->FetchPage(page_id[i]);
    bpm->UnpinPage(page_id[i], true);
    bpm->UnpinPage(page_id[i], true);
  }
  for (int i = 10; i < 20; i++) {
    bpm->NewPage(&page_id[i]);
    bpm->UnpinPage(page_id[i], false);
  }

  for (int i = 0; i < 10; i++) {
    bpm->FetchPage(page_id[i]);
  }
  bpm->UnpinPage(page_id[4], false);
  bpm->NewPage(&page_id[20]);
  bpm->FetchPage(page_id[5]);
  bpm->FetchPage(page_id[6]);
  bpm->FetchPage(page_id[7]);

  bpm->UnpinPage(page_id[5], false);
  bpm->UnpinPage(page_id[6], false);
  bpm->UnpinPage(page_id[7], false);

  bpm->UnpinPage(page_id[5], false);
  bpm->UnpinPage(page_id[6], false);
  bpm->UnpinPage(page_id[7], false);

  bpm->NewPage(&page_id[21]);  // evict 5
  bpm->FetchPage(page_id[5]);  // evict 6
  bpm->FetchPage(page_id[7]);
  bpm->FetchPage(page_id[6]);  // no evcit
  EXPECT_EQ(bpm->FetchPage(page_id[6]), nullptr);

  bpm->FetchPage(0);
  EXPECT_TRUE(bpm->UnpinPage(0, true));
  EXPECT_TRUE(bpm->UnpinPage(0, true));

  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerInstanceTest, ENABLE_SampleTest2) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 5;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManagerInstance(buffer_pool_size, disk_manager, k);
  page_id_t page_id[10];
  for (int i = 0; i < 10; i++) {
    bpm->NewPage(&page_id[i]);
  }
  bpm->FetchPage(0);
  EXPECT_TRUE(bpm->UnpinPage(0, true));
  EXPECT_TRUE(bpm->UnpinPage(0, true));

  delete bpm;
  delete disk_manager;
}
// NOLINTNEXTLINE
TEST(BufferPoolManagerInstanceTest, ENABLED_SampleTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 5;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManagerInstance(buffer_pool_size, disk_manager, k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);  // 0

  // Scenario: Once we have a page, we should be able to read and write content.
  snprintf(page0->GetData(), BUSTUB_PAGE_SIZE, "Hello");
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));  // 0 1 2 3 4 5 6 7 8 9 19
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new pages,
  // there would still be one buffer page left for reading page 0.
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(i, true));  // 0 1 2 3 4 |  5 6 7 8 9 10
  }
  for (int i = 0; i < 4; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));  // 0 1 2 3 4 |  5 6 7 8 9 10 |11 12 13 14
  }

  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm->FetchPage(0);
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Scenario: If we unpin page 0 and then make a new page, all the buffer pages should
  // now be pinned. Fetching page 0 should fail.
  EXPECT_EQ(true, bpm->UnpinPage(0, true));
  EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  EXPECT_EQ(nullptr, bpm->FetchPage(0));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

}  // namespace bustub
