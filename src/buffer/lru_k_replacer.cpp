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

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool { return false; }

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    no_evictable_set_.insert(frame_id);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    if (!set_evictable){
        no_evictable_set_.insert(frame_id);
    }

}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    BUSTUB_ASSERT(no_evictable_set_.count(frame_id),0);

}

auto LRUKReplacer::Size() -> size_t { return 0; }

template<class Key>
DoubleLinkedList<Key>::Node::Node(Key framd_id):pre_(nullptr), next_(nullptr), value(framd_id){

}
template <class Key>
DoubleLinkedList<Key>::Node::Node(Key frame_id, int vc): value(frame_id), visite_count(vc) {


}
template<class Key>
bustub::DoubleLinkedList<Key>::DoubleLinkedList(): size(0){
    head_ = new Node({});
    head_->pre_ = head_;
    head_->next_ = head_;
}
// namespace bustub
template<class Key>
auto bustub::DoubleLinkedList<Key>::InsertFront(Key frame_id) -> bool { 
    Node * temp = new Node(frame_id);
    return InsertFrontNode(temp);
}
template<class Key>
auto bustub::DoubleLinkedList<Key>::InsertFrontNode(Node * temp) -> bool { 
    temp->next_ = head_->next_;
    head_->next_->pre_ = temp;
    head_->next_ = temp;
    temp->pre_ = head_;
    size++;    
    return true; 
}
template<class Key>
auto bustub::DoubleLinkedList<Key>::RemoveTail(Key *frame_id) -> bool { 
    if (size==0){
        return false;
    }
    Node* tail = head_->pre_;
    *frame_id = tail->value;
    tail->pre_->next_ = tail->next_;
    tail->next_->pre_ = tail->pre_;
    delete tail;
    size--;
    return true;
}

template <class Key>
auto DoubleLinkedList<Key>::RemoveNodeFromList(Node *node) -> bool {
    if (node==head_){
        return false;
    }
    node->pre_->next_ = node->next_;
    node->next_->pre_ = node->pre_;
    return true;
}
template <class Key>
auto DoubleLinkedList<Key>::InsertOrdered(Node *node) -> bool {
  auto *temp = head_->next_;
  if (temp == head_) {
    InsertFrontNode(node);
    return true;
  }
  while (temp != head_ && temp->visite_count < node->visite_count) {
    temp = temp->next_;
  }
  temp->pre_->next_ = node;
  node->next_ = temp;
  node->pre_ = temp->pre_;
  temp->pre_ = node;
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
template class DoubleLinkedList<frame_id_t>;
}  // namespace bustub

bustub::LRU_K::LRU_K(int size, int K):capacity(size),k(K) {

}

void bustub::LRU_K::access(frame_id_t id) {
  // 先访问第一层cache
  if (LRU_map.count(id)) {
    // 如果命中
    Node *node = LRU_map[id];
    lru_cache.RemoveNodeFromList(node);
    lru_cache.InsertFrontNode(node);

    }
    // 再访问第二层cache
    else if(history_map.count(id)){
        //如果命中
        Node *node = history_map[id];
        node->visite_count++;
        // 如果访问次数满k次了
        if (node->visite_count==k){

        }else{
            history_cache.RemoveNodeFromList(node);
            history_cache.InsertOrdered(node);
        }
    }

    // 如果两层
}