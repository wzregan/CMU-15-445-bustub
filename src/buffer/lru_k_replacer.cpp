//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : replacer_size_(num_frames), k_(k), lru_k_(num_frames, k), evict_able_size_(0), evict_disable_size_(0) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock sl(this->latch_);
  bool ret = lru_k_.Evict(frame_id);
  if (ret) {
    this->evict_able_size_--;
  }
  return ret;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock sl(this->latch_);
  BUSTUB_ASSERT(evict_able_size_ + evict_disable_size_ < static_cast<int>(replacer_size_), "LRK out of specific size");
  if (!lru_k_.Contained(frame_id)) {
    this->evict_able_size_++;
  }
  lru_k_.Access(frame_id);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock sl(this->latch_);
  if (!lru_k_.SetEvictable(frame_id, set_evictable)) {
    return;
  }
  if (!set_evictable) {
    this->evict_disable_size_++;
    this->evict_able_size_--;
  } else {
    this->evict_disable_size_--;
    this->evict_able_size_++;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock sl(this->latch_);
  // BUSTUB_ASSERT(no_evictable_set_.count(frame_id),0);
  this->SetEvictable(frame_id, true);

  if (lru_k_.Remove(frame_id)) {
    this->evict_able_size_--;
  }
}

auto LRUKReplacer::Size() -> size_t { return this->evict_able_size_; }

template <class Key>
DoubleLinkedList<Key>::Node::Node(Key frame_id) : pre_(nullptr), next_(nullptr), value_(frame_id) {}
template <class Key>
DoubleLinkedList<Key>::Node::Node(Key frame_id, int vc) : value_(frame_id), visite_count_(vc) {}
template <class Key>
bustub::DoubleLinkedList<Key>::DoubleLinkedList() {
  head_ = new Node({});
  head_->pre_ = head_;
  head_->next_ = head_;
}
// namespace bustub
template <class Key>
auto bustub::DoubleLinkedList<Key>::InsertFront(Key frame_id) -> bool {
  Node *temp = new Node(frame_id);
  return InsertFrontNode(temp);
}
template <class Key>
auto bustub::DoubleLinkedList<Key>::InsertTail(Key frame_id) -> bool {
  Node *temp = new Node(frame_id);
  return InsertTailNode(temp);
}

template <class Key>
auto bustub::DoubleLinkedList<Key>::InsertFrontNode(Node *temp) -> bool {
  temp->next_ = head_->next_;
  head_->next_->pre_ = temp;
  head_->next_ = temp;
  temp->pre_ = head_;
  size_++;
  return true;
}
template <class Key>
auto bustub::DoubleLinkedList<Key>::InsertTailNode(Node *temp) -> bool {
  head_->pre_->next_ = temp;
  temp->pre_ = head_->pre_;
  head_->pre_ = temp;
  temp->next_ = head_;
  size_++;
  return true;
}
template <class Key>
auto bustub::DoubleLinkedList<Key>::RemoveTail(Key *frame_id) -> bool {
  if (size_ == 0) {
    return false;
  }
  Node *tail = head_->pre_;
  *frame_id = tail->value_;
  tail->pre_->next_ = tail->next_;
  tail->next_->pre_ = tail->pre_;
  delete tail;
  size_--;
  return true;
}

template <class Key>
auto DoubleLinkedList<Key>::RemoveNodeFromList(Node *node) -> bool {
  if (node == head_) {
    return false;
  }
  node->pre_->next_ = node->next_;
  node->next_->pre_ = node->pre_;
  node->pre_ = nullptr;
  node->next_ = nullptr;
  size_--;
  return true;
}

template <class Key>
auto DoubleLinkedList<Key>::FindFirstEvictableNode() -> Node * {
  auto temp = this->head_->next_;
  while (temp != this->head_) {
    if (temp->evictable_) {
      return temp;
    }
    temp = temp->next_;
  }
  return nullptr;
}

template <class Key>
auto DoubleLinkedList<Key>::InsertOrdered(Node *node) -> bool {
  auto *temp = head_->next_;
  if (temp == head_) {
    InsertFrontNode(node);
    return true;
  }
  while (temp != head_ && temp->visite_count_ <= node->visite_count_) {
    temp = temp->next_;
  }
  temp->pre_->next_ = node;
  node->next_ = temp;
  node->pre_ = temp->pre_;
  temp->pre_ = node;
  size_++;
  return true;
}
template <class Key>
bustub::DoubleLinkedList<Key>::~DoubleLinkedList() {
  Node *delted_node = head_->next_;
  Node *temp = delted_node->next_;

  while (delted_node != head_) {
    temp = delted_node->next_;
    delete delted_node;
    delted_node = temp;
  }
  delete head_;
}
}  // namespace bustub

bustub::LruK::LruK(int size, int K) : capacity_(size), k_(K) {}

void bustub::LruK::Access(frame_id_t id) {
  // 先访问第一层cache
  if (lru_map_.count(id) != 0) {
    // 如果命中
    Node *node = lru_map_[id];
    lru_cache_.RemoveNodeFromList(node);
    lru_cache_.InsertTailNode(node);
  } else if (history_map_.count(id) != 0) {
    // 如果没有命中，再访问第二层cache
    // 如果命中
    Node *node = history_map_[id];
    node->visite_count_++;
    // 如果访问次数满k次了
    if (node->visite_count_ == k_) {
      history_cache_.RemoveNodeFromList(node);  // 将其从第一层cache移掉
      lru_cache_.InsertTailNode(node);          // 将其插入到第二层cache中
      lru_map_[id] = node;
      history_map_.erase(id);
    } else {
      history_cache_.RemoveNodeFromList(node);
      history_cache_.InsertOrdered(node);
    }
  } else {
    // 如果两层cache都没有命中，那么就执行插入操作
    // 先判断cache是否满
    if (IsFull()) {
      // 如果满了，则需要逐出元素
      return;
    }
    // 如果没有满，则插入
    Node *new_node = new Node(id, 1);
    history_cache_.InsertOrdered(new_node);
    history_map_[id] = new_node;
  }
  // 如果两层
}

auto bustub::LruK::Evict(frame_id_t *id) -> bool {
  // 先从历史链表中寻找
  if (history_cache_.Size() > 0) {
    Node *ret = history_cache_.FindFirstEvictableNode();  // 从头开始找，找到第一个可以驱逐的结点，然后驱逐
    if (ret != nullptr) {
      *id = ret->value_;
      history_cache_.RemoveNodeFromList(ret);
      history_map_.erase(ret->value_);
      delete ret;
      return true;
    }
  }
  // 没有的话，再从lru_cache中寻找
  if (lru_cache_.Size() > 0) {
    Node *ret = lru_cache_.FindFirstEvictableNode();
    if (ret != nullptr) {
      *id = ret->value_;
      lru_cache_.RemoveNodeFromList(ret);
      lru_map_.erase(ret->value_);
      delete ret;
      return true;
    }
  }
  return false;
}
auto bustub::LruK::Contained(frame_id_t id) -> bool { return lru_map_.count(id) > 0 || history_map_.count(id) > 0; }
auto bustub::LruK::SetEvictable(frame_id_t id, bool evictable) -> bool {
  // 如果 直接在历史缓存区里找到
  bool flag = false;
  if (history_map_.count(id) > 0) {
    Node *temp = history_map_[id];
    flag = (temp->evictable_ != evictable);
    temp->evictable_ = evictable;
  } else if (lru_map_.count(id) > 0) {
    Node *temp = lru_map_[id];
    flag = (temp->evictable_ != evictable);
    temp->evictable_ = evictable;
  }
  return flag;
}

auto bustub::LruK::Remove(frame_id_t id) -> bool {
  if (history_map_.count(id) > 0) {
    Node *temp = history_map_[id];
    history_map_.erase(id);
    history_cache_.RemoveNodeFromList(temp);
    return true;
  } 
  if (lru_map_.count(id) > 0) {
    Node *temp = lru_map_[id];
    lru_map_.erase(id);
    lru_cache_.RemoveNodeFromList(temp);
    return true;
  }
  return false;
}

auto bustub::LruK::Size() -> int { return lru_cache_.Size() + history_cache_.Size(); }

auto bustub::LruK::IsFull() -> bool { return Size() >= capacity_; }

template class bustub::DoubleLinkedList<int>;
