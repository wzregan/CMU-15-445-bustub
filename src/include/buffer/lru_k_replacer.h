//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <set>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/macros.h"
namespace bustub {
/**
 *
 *
 */
template <class Key>
class DoubleLinkedList {
 public:
  class Node {
   public:
    Node *pre_;
    Node *next_;
    Key value_;
    bool evictable_{true};
    int visite_count_{0};
    explicit Node(Key frame_id);
    Node(Key frame_id, int vc);
  };

 public:
  DoubleLinkedList();

 public:
  Node *head_;
  int size_{0};
  auto InsertFront(Key frame_id) -> bool;
  auto InsertTail(Key frame_id) -> bool;

  auto InsertTailNode(Node *temp) -> bool;
  auto InsertFrontNode(Node *temp) -> bool;
  auto RemoveTail(Key *frame_id) -> bool;
  auto RemoveNodeFromList(Node *node) -> bool;
  auto FindFirstEvictableNode() -> Node *;
  inline auto Size() -> int { return size_; }
  auto InsertOrdered(Node *node) -> bool;

  ~DoubleLinkedList();
};

class LruK {
  using Node = DoubleLinkedList<frame_id_t>::Node;
  using LRU_List = DoubleLinkedList<frame_id_t>;
  using History_List = DoubleLinkedList<frame_id_t>;
  using Map = std::unordered_map<frame_id_t, Node *>;

  int capacity_;
  int k_;

  LRU_List lru_cache_;
  History_List history_cache_;
  Map history_map_;
  Map lru_map_;

 public:
  LruK(int size, int K);
  void Access(frame_id_t id);
  auto Evict(frame_id_t *id) -> bool;
  auto Contained(frame_id_t id) -> bool;
  auto SetEvictable(frame_id_t id, bool evictable) -> bool;
  auto Remove(frame_id_t id) -> bool;

  auto Size() -> int;
  auto IsFull() -> bool;
};

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time
 * between current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward
 * k-distance, classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be
   * required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that
   * frame. Only frames that are marked as 'evictable' are candidates for
   * eviction.
   *
   * A frame with less than k historical references is given +inf as its
   * backward k-distance. If multiple frames have inf backward k-distance, then
   * evict the frame with the earliest timestamp overall.
   *
   * Successful eviction of a frame should decrement the size of replacer and
   * remove the frame's access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be
   * evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current
   * timestamp. Create a new entry for access history if frame id has not been
   * seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an
   * exception. You can also use BUSTUB_ASSERT to abort the process if frame id
   * is invalid.
   *
   * @param frame_id id of frame that received a new access.
   */
  void RecordAccess(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function
   * also controls replacer's size. Note that size is equal to number of
   * evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then
   * size should decrement. If a frame was previously non-evictable and is to be
   * set to evictable, then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying
   * anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access
   * history. This function should also decrement replacer's size if removal is
   * successful.
   *
   * Note that this is different from evicting a frame, which always remove the
   * frame with largest backward k-distance. This function removes specified
   * frame id, no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort
   * the process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t;

 private:
  // TODO(student): implement me! You can replace these member variables as you
  // like. Remove maybe_unused if you start using them.
  [[maybe_unused]] size_t current_timestamp_{0};
  [[maybe_unused]] size_t curr_size_{0};
  [[maybe_unused]] size_t replacer_size_;
  [[maybe_unused]] size_t k_;
  std::mutex latch_;
  LruK lru_k_;
  int evict_able_size_;
  int evict_disable_size_;
};

}  // namespace bustub
