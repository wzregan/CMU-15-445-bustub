//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  /**
   * 对BufferPoolManager进行初始化操作
   */
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];                                               // 初始化page数组，连续的内存
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);  // 初始化page_id -> frame_id 的 映射表
  replacer_ = new LRUKReplacer(pool_size, 2);                                  // LRUK缓存策略

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }  // free_list 存放了所有可用的frame_id

  // TODO(students): remove this line after you have implemented the buffer pool
  // manager throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished
  //     implementing BPM, please remove the throw " "exception line in
  //     `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  // 如果没有可以使用的页框 且 没有可驱逐的页面，则直接返回
  if (this->free_list_.empty() && this->replacer_->Size() == 0) {
    return nullptr;
  }
  frame_id_t f_id;
  if (!this->free_list_.empty()) {
    // 首先考虑从空闲框中创建一个页
    f_id = this->free_list_.back();
    this->free_list_.pop_back();
    page_id_t new_page_id = AllocatePage();  // 分配一个新的page_id
    page_table_->Insert(new_page_id, f_id);
    pages_[f_id].page_id_ = new_page_id;
    replacer_->RecordAccess(f_id);
    replacer_->SetEvictable(f_id, false);
    pages_[f_id].pin_count_++;
    *page_id = new_page_id;
    return &pages_[f_id];
  } 
  // 如果free_list中没有，那么我们就驱逐一个页
  replacer_->Evict(&f_id);
  page_id_t p_id = this->pages_[f_id].GetPageId();
  this->DeletePgImp(p_id);  // 删除这个页面
  return NewPgImp(page_id);
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  frame_id_t frame_id;
  bool find_able = page_table_->Find(page_id, frame_id);
  if (find_able) {
    return &this->pages_[frame_id];
  }
  // 如果根本没有缓存这个page

  // 1. 从free_list中寻找空位，然后将其加入到缓存池中
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
    // 有了空位之后然后开始读取
    disk_manager_->ReadPage(page_id, this->pages_[frame_id].GetData());
    this->pages_[frame_id].page_id_ = page_id;
    // 加入到映射中
    page_table_->Insert(page_id, frame_id);
    // 读取完之后pincount++就可以直接返回了
    this->replacer_->RecordAccess(frame_id);
    this->pages_[frame_id].pin_count_++;
    this->replacer_->SetEvictable(frame_id, false);
    return &this->pages_[frame_id];
  } 
  
  if (this->replacer_->Size() > 0) {
    // 如果有可以驱逐的，则先驱逐掉原来的page，然后重新读取数据进page中
    this->replacer_->Evict(&frame_id);
    page_id_t old_page = this->pages_[frame_id].GetPageId();
    this->DeletePgImp(old_page);
    return FetchPgImp(page_id);
  }
  return nullptr;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  frame_id_t frame_id;
  bool find_able = page_table_->Find(page_id, frame_id);
  if (!find_able) {
    // 如果找到的话
    return false;
  }
  // 先看pin值是否为0，如果为0则直接返回
  if (this->pages_[frame_id].pin_count_ > 0) {
    this->pages_[frame_id].pin_count_--;
    this->pages_[frame_id].is_dirty_ = is_dirty;
  } else {
    return false;
  }
  // 更新完pin数值之后，然后对更新evictable
  if (this->pages_[frame_id].pin_count_ == 0) {
    this->replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  frame_id_t frame_id;
  bool find_able = this->page_table_->Find(page_id, frame_id);
  // 如果这个页不存在
  if (!find_able) {
    return false;
  }
  disk_manager_->WritePage(page_id, this->pages_[frame_id].GetData());

  this->pages_[frame_id].is_dirty_ = false;

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (int i = 0; i < static_cast<int>(pool_size_); i++) {
    if (this->pages_[i].GetPageId() != INVALID_PAGE_ID) {
      this->FlushPage(this->pages_[i].GetPageId());
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  frame_id_t frame_id;
  bool find_able = page_table_->Find(page_id, frame_id);
  if (!find_able) {
    // 如果没有找到，则直接返回false
    return false;
  }
  // 从hash中删除page_id的映射
  if (this->pages_[frame_id].is_dirty_) {
    FlushPgImp(page_id);
  }
  page_table_->Remove(page_id);
  this->pages_[frame_id].is_dirty_ = false;

  // 然后将空闲的frame添加进对应的free_list中
  free_list_.push_back(frame_id);
  this->pages_[frame_id].is_dirty_ = false;
  this->pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  this->pages_[frame_id].SetLSN(0);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
