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

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k),
lru_k(num_frames, k), evict_able_size(0), evict_disable_size(0) {

}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool { 
    std::scoped_lock sl(this->latch_);
    bool ret = lru_k.evict(frame_id);
    if (ret){
        this->evict_able_size--;
    }
    return ret; 
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    std::scoped_lock sl(this->latch_);
    BUSTUB_ASSERT(evict_able_size + evict_disable_size < static_cast<int>(replacer_size_), "LRK out of specific size");
    if (!lru_k.contained(frame_id)){
        this->evict_able_size++;
    }
    lru_k.access(frame_id);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::scoped_lock sl(this->latch_);
    if(!lru_k.SetEvictable(frame_id,set_evictable))
        return;
    if(!set_evictable){
        this->evict_disable_size++;
        this->evict_able_size--;
    }else{
        this->evict_disable_size--;
        this->evict_able_size++;
    }

    
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    std::scoped_lock sl(this->latch_);
    // BUSTUB_ASSERT(no_evictable_set_.count(frame_id),0);
    this->SetEvictable(frame_id, true);
    
    if (lru_k.Remove(frame_id)){
        this->evict_able_size--;    
    }
    
}

auto LRUKReplacer::Size() -> size_t { return this->evict_able_size; }

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
auto bustub::DoubleLinkedList<Key>::InsertTail(Key frame_id) -> bool{
    Node * temp = new Node(frame_id);
    return InsertTailNode(temp);
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
auto bustub::DoubleLinkedList<Key>::InsertTailNode(Node * temp) -> bool { 
    head_->pre_->next_ = temp;
    temp->pre_ = head_->pre_;
    head_->pre_ = temp;
    temp->next_ = head_;
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
    node->pre_=nullptr;
    node->next_=nullptr;
    size--;
    return true;
}

template <class Key>
auto DoubleLinkedList<Key>::FindFirstEvictableNode() -> Node * {
    auto temp = this->head_->next_;
    while (temp!=this->head_){
        if (temp->evictable){
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
  while (temp != head_ && temp->visite_count <= node->visite_count) {
    temp = temp->next_;
  }
  temp->pre_->next_ = node;
  node->next_ = temp;
  node->pre_ = temp->pre_;
  temp->pre_ = node;
  size++;
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

bustub::LRU_K::LRU_K(int size, int K):capacity(size),k(K) {

}

void bustub::LRU_K::access(frame_id_t id) {
  // 先访问第一层cache
    if (LRU_map.count(id)) {
    // 如果命中
        Node *node = LRU_map[id];
        lru_cache.RemoveNodeFromList(node);
        lru_cache.InsertTailNode(node);
    }
    // 如果没有命中，再访问第二层cache
    else if(history_map.count(id)){
        // 如果命中
        Node *node = history_map[id];
        node->visite_count++;
        // 如果访问次数满k次了
        if (node->visite_count==k){
            history_cache.RemoveNodeFromList(node); // 将其从第一层cache移掉
            lru_cache.InsertTailNode(node); // 将其插入到第二层cache中
            LRU_map[id] = node;
            history_map.erase(id);
        }else{
            history_cache.RemoveNodeFromList(node);
            history_cache.InsertOrdered(node);
        }
    }else{
        // 如果两层cache都没有命中，那么就执行插入操作
        // 先判断cache是否满
        if(IsFull()){
            // 如果满了，则需要逐出元素
            return;
        }else{ 
            // 如果没有满，则插入
            Node * new_node = new Node(id, 1);
            history_cache.InsertOrdered(new_node);
            history_map[id] = new_node;
        }
    }
    // 如果两层
}

bool bustub::LRU_K::evict(frame_id_t *id) {
    // 先从历史链表中寻找
    if (history_cache.Size() > 0){
        Node *ret = history_cache.FindFirstEvictableNode(); // 从头开始找，找到第一个可以驱逐的结点，然后驱逐
        if (ret != nullptr){
            *id = ret->value;
            history_cache.RemoveNodeFromList(ret);
            history_map.erase(ret->value);
            delete ret;
            return true;
        }
    }
    // 没有的话，再从lru_cache中寻找
    if (lru_cache.Size() > 0){
        Node *ret = lru_cache.FindFirstEvictableNode();
        if (ret != nullptr){
            *id = ret->value;
            lru_cache.RemoveNodeFromList(ret);
            LRU_map.erase(ret->value);
            delete ret;
            return true;
        }
    }
    return false;
}
bool bustub::LRU_K::contained(frame_id_t id) { 
    return LRU_map.count(id) || history_map.count(id);
}
bool bustub::LRU_K::SetEvictable(frame_id_t id, bool evictable) {
  // 如果 直接在历史缓存区里找到
  bool flag = false;
  if (history_map.count(id) > 0) {
    Node *temp = history_map[id];
    flag = (temp->evictable != evictable);
    temp->evictable = evictable;
  } else if (LRU_map.count(id) > 0) {
    Node *temp = LRU_map[id];
    flag = (temp->evictable != evictable);
    temp->evictable = evictable;
  }
  return flag;
}

bool bustub::LRU_K::Remove(frame_id_t id) { 
    if (history_map.count(id) > 0) {
        Node *temp = history_map[id];
        history_map.erase(id);
        history_cache.RemoveNodeFromList(temp);
        return true;
    } else if (LRU_map.count(id) > 0) {
        Node *temp = LRU_map[id];
        LRU_map.erase(id);
        lru_cache.RemoveNodeFromList(temp);
        return true;
    }
    return false; 
}


int bustub::LRU_K::size() { return lru_cache.Size() + history_cache.Size(); }

bool bustub::LRU_K::IsFull() { 
    return size() >= capacity; 
}

template class bustub::DoubleLinkedList<int>;
