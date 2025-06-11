//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"
#include "common/logger.h"
namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new LRUKReplacer.
 * @param num_frames the maximum number of frames the LRUReplacer will be required to store
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

/**
 * TODO(P1): Add implementation
 *
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return the frame ID if a frame is successfully evicted, or `std::nullopt` if no frames can be evicted.
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {  
    std::lock_guard<std::mutex> lock(latch_);
    if (curr_size_ == 0) {
        return std::nullopt;  // No frames to evict
    }
    frame_id_t victim_frame_id = -1;
    size_t max_backward_distance = 0;
    size_t oldest_timestamp = std::numeric_limits<size_t>::max();
    for(const auto &pair : node_store_) {
        const auto &node = pair.second;
        if(!node.is_evictable_) {
            continue;  // Skip non-evictable frames
        }
        size_t backward_distance = node.BackwardKDistance(current_timestamp_);
        // LOG_DEBUG("Checking frame_id: %d, backward distance: %lu, history size: %lu recent timestamp: %lu",
        //           node.fid_, backward_distance, node.history_.size(), node.history_.front());
        if (backward_distance > max_backward_distance ||
            (backward_distance == max_backward_distance && node.history_.front() < oldest_timestamp)) {
            max_backward_distance = backward_distance;
            victim_frame_id = node.fid_;
            oldest_timestamp = node.history_.front();
        }
    }
    // if we found a victim frame, remove it from the store
    if (victim_frame_id != -1) {
        auto it = node_store_.find(victim_frame_id);
        if (it != node_store_.end()) {
            node_store_.erase(it);  // Remove the victim frame from the store
            curr_size_--;  // Decrement the size of the replacer
            // LOG_DEBUG("Evicted frame_id: %d, current size: %lu", victim_frame_id, curr_size_);
        }
    }
    return victim_frame_id == -1 ? std::nullopt : std::make_optional(victim_frame_id);
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
    if(static_cast<size_t>(frame_id) >= replacer_size_) {
        throw Exception(ExceptionType::INVALID, "Invalid frame id: " + std::to_string(frame_id));
    }
    std::lock_guard<std::mutex> lock(latch_);
    current_timestamp_++;  // Increment the global timestamp for each access
    // if(access_type == AccessType::Unknown) {
    //     throw Exception(ExceptionType::INVALID, "Access type cannot be Unknown");
    // }
    if(node_store_.find(frame_id) == node_store_.end()) {
        // Create a new node if it doesn't exist
        node_store_[frame_id] = LRUKNode(frame_id,k_);
        node_store_[frame_id].history_.push_front(current_timestamp_);
    }else{
        // Update existing node
        auto &node = node_store_[frame_id];
        node.history_.push_front(current_timestamp_);
        if (node.history_.size() > k_) {
            node.history_.pop_back();  // Maintain only the last k accesses
        }
    }
    // LOG_DEBUG("Recorded access for frame_id: %d, current timestamp: %lu, last recent timestamp: %lu",frame_id, current_timestamp_, node_store_[frame_id].history_.front());
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    if(static_cast<size_t>(frame_id) >= replacer_size_) {
        throw Exception(ExceptionType::INVALID, "Invalid frame id: " + std::to_string(frame_id));
    }
    std::lock_guard<std::mutex> lock(latch_);
    if (node_store_.find(frame_id) == node_store_.end()) {
        // Frame does not exist, nothing to do
        return;
    }
    auto &node = node_store_[frame_id];
    if (set_evictable && !node.is_evictable_) {
        // Frame is being set to evictable
        node.is_evictable_ = true;
        curr_size_++;
        // LOG_DEBUG("Set frame_id: %d as evictable, current size: %lu", frame_id, curr_size_);
    } else if (!set_evictable && node.is_evictable_) {
        // Frame is being set to non-evictable
        node.is_evictable_ = false;
        curr_size_--;
        // LOG_DEBUG("Set frame_id: %d as non-evictable, current size: %lu", frame_id, curr_size_);
    }

}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
    if(static_cast<size_t>(frame_id) >= replacer_size_) {
        throw Exception(ExceptionType::INVALID, "Invalid frame id: " + std::to_string(frame_id));
    }
    std::lock_guard<std::mutex> lock(latch_);
    auto it = node_store_.find(frame_id);
    if (it == node_store_.end()) {
        // Frame not found, nothing to do
        return;
    }
    auto &node = it->second;
    if (!node.is_evictable_) {
        throw Exception(ExceptionType::INVALID, "Frame is not evictable: " + std::to_string(frame_id));
    }
    // Remove from stores
    node_store_.erase(it);
    curr_size_--;
    // LOG_DEBUG("Removed frame_id: %d, current size: %lu", frame_id, curr_size_);
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t { 
    std::lock_guard<std::mutex> lock(latch_);
    return curr_size_; 
}

}  // namespace bustub
