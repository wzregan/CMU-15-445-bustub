#include "container/hash/extendible_hash_table.h"
#include <memory>
#include "gtest/gtest.h"
#include <string>
using std::string;

TEST(HASH_TABLE_BUCKET_TEST, DISABLED_INSERT_TEST){
    bustub::ExtendibleHashTable<string,int>::Bucket bucket((size_t)2,1);
    EXPECT_EQ(bucket.GetCapacity(), 2);
    EXPECT_TRUE(bucket.Insert("height",189));
    int ret;
    EXPECT_TRUE(bucket.Find("height",ret));
    EXPECT_EQ(ret,189);

    EXPECT_TRUE(bucket.Insert("height",188));
    EXPECT_TRUE(bucket.Find("height",ret));
    EXPECT_EQ(ret,188);

    EXPECT_FALSE(bucket.IsFull());
    EXPECT_TRUE(bucket.Insert("name",111));
    EXPECT_TRUE(bucket.IsFull());
    EXPECT_TRUE(bucket.contained("height"));
}
TEST(HASH_TABLE_BUCKET_TEST, DISABLED_HASHTAB_TEST){
    bustub::ExtendibleHashTable<string,int> tab(2L);
    tab.Insert("age",18);
    tab.Insert("ID",2022);
    tab.Insert("age",19);
    int v;
    EXPECT_TRUE(tab.Find("age",v));
    EXPECT_EQ(v,19);
    EXPECT_TRUE(tab.Remove("ID"));
    EXPECT_FALSE(tab.Remove("ID"));
    EXPECT_FALSE(tab.Find("height",v));
}

TEST(HASH_TABLE_BUCKET_TEST, DISABLED_BUCKET_SPLITE_TEST){
    bustub::ExtendibleHashTable<string,int> tab(2L);
    tab.dir_ = std::vector<std::shared_ptr<bustub::ExtendibleHashTable<string,int>::Bucket>>(4);
    tab.global_depth_ = 2;
    auto temp1 = std::make_shared<bustub::ExtendibleHashTable<string,int>::Bucket>(2,2,0);
    auto temp2 = std::make_shared<bustub::ExtendibleHashTable<string,int>::Bucket>(2,2,2);
    auto temp3 = std::make_shared<bustub::ExtendibleHashTable<string,int>::Bucket>(2,1,1);
    tab.dir_[0] = temp1;
    tab.dir_[1] = temp3;
    tab.dir_[2] = temp2;
    tab.dir_[3] = temp3;
    temp3->IncreaseDepth();
    EXPECT_EQ(temp3->GetDepth(), 2);
    auto old_bucket = tab.SplitBucket(1);
    EXPECT_EQ(old_bucket.get(), temp3.get());
    EXPECT_NE(tab.dir_[1], tab.dir_[3]);
}
TEST(HASH_TABLE_BUCKET_TEST, ENABLE_TAB_EXTENDDIRECTORY_TEST){
    bustub::ExtendibleHashTable<string,int> tab(2L);
    tab.dir_ = std::vector<std::shared_ptr<bustub::ExtendibleHashTable<string,int>::Bucket>>(4);
    tab.global_depth_ = 2;
    auto temp1 = std::make_shared<bustub::ExtendibleHashTable<string,int>::Bucket>(2,2,0);
    auto temp2 = std::make_shared<bustub::ExtendibleHashTable<string,int>::Bucket>(2,2,2);
    auto temp3 = std::make_shared<bustub::ExtendibleHashTable<string,int>::Bucket>(2,1,1);
    tab.dir_[0] = temp1;
    tab.dir_[1] = temp3;
    tab.dir_[2] = temp2;
    tab.dir_[3] = temp3;
    int depth = tab.ExtendDirectory();
    EXPECT_EQ(depth,3);
    EXPECT_EQ(tab.dir_.size(),8);
    EXPECT_EQ(tab.dir_[1],tab.dir_[3]);
    EXPECT_EQ(tab.dir_[0],tab.dir_[4]);
    EXPECT_EQ(tab.dir_[1],tab.dir_[5]);
    EXPECT_EQ(tab.dir_[2],tab.dir_[6]);
    EXPECT_EQ(tab.dir_[3],tab.dir_[7]);
    EXPECT_EQ(tab.dir_.size(),8);


}

TEST(HASH_TABLE_BUCKET_TEST, ENABLE_TAB_INSERT){
    bustub::ExtendibleHashTable<string,int> tab(2L);
    EXPECT_EQ(tab.GetNumBuckets(),2);
    tab.Insert("age",18);
    int age,height,id;
    bool ret = tab.Find("age",age);
    EXPECT_EQ(age,18);
    EXPECT_TRUE(ret);
    ret = tab.Find("height",height);
    EXPECT_FALSE(ret);
    tab.Insert("age",21);
    ret = tab.Find("age",age);
    EXPECT_EQ(age,21);
    EXPECT_TRUE(ret);
    tab.Insert("height",188);
    EXPECT_EQ(tab.GetNumBuckets(),3);
    tab.Insert("id",2029);
    EXPECT_EQ(tab.GetNumBuckets(),3);
    EXPECT_EQ(tab.dir_.size(),4);
    ret = tab.Find("age",age);
    EXPECT_TRUE(ret);
    EXPECT_EQ(age,21);
    
    ret = tab.Find("height",height);
    EXPECT_TRUE(ret);
    EXPECT_EQ(height,188);

    ret = tab.Find("id",id);
    EXPECT_TRUE(ret);
    EXPECT_EQ(id,2029);

    ret = tab.Remove("id");
    EXPECT_TRUE(ret);

    ret = tab.Find("id",id);
    EXPECT_TRUE(ret);

    tab.Insert("id1",2029);
    tab.Insert("id2",2029);
    tab.Insert("id3",2029);
    tab.Insert("id4",2029);
    tab.Insert("id5",2029);


}
