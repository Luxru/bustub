//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog_presto.h
//
// Identification: src/include/primer/hyperloglog_presto.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <bitset>
#include <memory>
#include <mutex>  // NOLINT
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"

/** @brief Dense bucket size. */
#define DENSE_BUCKET_SIZE 4
/** @brief Overflow bucket size. */
#define OVERFLOW_BUCKET_SIZE 3

/** @brief Total bucket size. */
#define TOTAL_BUCKET_SIZE (DENSE_BUCKET_SIZE + OVERFLOW_BUCKET_SIZE)

namespace bustub {

template <typename KeyType>
class HyperLogLogPresto {
  /**
   * INSTRUCTIONS: Testing framework will use the GetDenseBucket and GetOverflow function,
   * hence SHOULD NOT be deleted. It's essential to use the dense_bucket_
   * data structure.
   */

  /** @brief Constant for HLL. */
  static constexpr double CONSTANT = 0.79402;

 public:
  /** @brief Disabling default constructor. */
  HyperLogLogPresto() = delete;

  explicit HyperLogLogPresto(int16_t n_leading_bits);

  /** @brief Returns the dense_bucket_ data structure. */
  auto GetDenseBucket() const -> std::vector<std::bitset<DENSE_BUCKET_SIZE>> { return dense_bucket_; }

  /** @brief Returns overflow bucket of a specific given index. */
  auto GetOverflowBucketofIndex(uint16_t idx) { return overflow_bucket_[idx]; }

  /** @brief Returns the cardinality of the set. */
  auto GetCardinality() const -> uint64_t { return cardinality_; }

  auto AddElem(KeyType val) -> void;

  auto ComputeCardinality() -> void;

 private:
  /** @brief Calculate Hash.
   *
   * @param[in] val
   *
   * @returns hash value
   */
  inline auto CalculateHash(KeyType val) -> hash_t {
    Value val_obj;
    if constexpr (std::is_same<KeyType, std::string>::value) {
      val_obj = Value(VARCHAR, val);
      return bustub::HashUtil::HashValue(&val_obj);
    }
    if constexpr (std::is_same<KeyType, int64_t>::value) {
      return static_cast<hash_t>(val);
    }
    return 0;
  }

  /**
   * @brief Function to insert to bucket.
   * @param[in] index - index of the bucket
   * @param[in] bucket_value - value of the bucket
   * @returns void
   */
   inline auto InsertToBucket(const uint64_t index, const uint64_t bucket_value) -> void {
      // find the index of the bucket
      if(GetFromBucket(index)>bucket_value){
        // no need to insert
        return;
      }
      if(bucket_value > (1<<DENSE_BUCKET_SIZE) -1){
        // has overflowed
        std::bitset<DENSE_BUCKET_SIZE> bset_dense(bucket_value & ((1 << DENSE_BUCKET_SIZE) - 1));
        std::bitset<OVERFLOW_BUCKET_SIZE> bset_overflow(bucket_value >> DENSE_BUCKET_SIZE);
        dense_bucket_.at(index) = bset_dense;
        overflow_bucket_[static_cast<uint16_t>(index)] = bset_overflow;
      }else{
        // has not overflowed
        std::bitset<DENSE_BUCKET_SIZE> bset_dense(bucket_value);
        dense_bucket_.at(index) = bset_dense;
      }
   }

  /**
   * @brief Function compute bucket value.
   * @param[in] index - index of the bucket
   * @returns bucket value
   */
   inline auto GetFromBucket(const uint64_t index) -> uint64_t {
    if(overflow_bucket_.find(index) != overflow_bucket_.end()){
      // need concatenate the two bitsets
      std::bitset<DENSE_BUCKET_SIZE> bset_dense = dense_bucket_.at(index);
      std::bitset<OVERFLOW_BUCKET_SIZE> bset_overflow = overflow_bucket_.at(static_cast<uint16_t>(index));
      uint16_t bset_total = 0;
      bset_total |= bset_dense.to_ulong();
      bset_total |= (bset_overflow.to_ulong() << DENSE_BUCKET_SIZE);
      return bset_total;
    }      
    // no need to concatenate the two bitsets
    std::bitset<DENSE_BUCKET_SIZE> bset_dense = dense_bucket_.at(index);
    return bset_dense.to_ulong();
  }
   

  /** @brief Structure holding dense buckets (or also known as registers). */
  std::vector<std::bitset<DENSE_BUCKET_SIZE>> dense_bucket_;

  /** @brief Structure holding overflow buckets. */
  std::unordered_map<uint16_t, std::bitset<OVERFLOW_BUCKET_SIZE>> overflow_bucket_;

  /** @brief Storing cardinality value */
  uint64_t cardinality_;
  uint64_t inital_bits_;

  // TODO(student) - can add more data structures as required
};

}  // namespace bustub
