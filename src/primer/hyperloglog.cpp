//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog.cpp
//
// Identification: src/primer/hyperloglog.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/hyperloglog.h"
#include <bitset>
#include <cmath>
#include <cstdint>
#include "common/logger.h"
namespace bustub {

/** @brief Parameterized constructor. */
template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits) : cardinality_(0),inital_bits_(n_bits) {
  // calculate the size of the dense bucket
  if(n_bits<0){
    n_bits = 0;
  }
  uint64_t size = (1 << n_bits );//size = 2^n_bits
  dense_bucket_.resize(size);
  // initialize the dense bucket
  for (uint64_t i = 0; i < size; i++) {
    dense_bucket_[i] = 0;
  }
  LOG_INFO("HyperLogLog initialized with %d bits, size: %ld", n_bits, (long)size);
}

/**
 * @brief Function that computes binary.
 *
 * @param[in] hash
 * @returns binary of a given hash
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  /** @TODO(student) Implement this function! */
  std::bitset<BITSET_CAPACITY> bset(static_cast<uint64_t>(hash));  
  return bset;
}

/**
 * @brief Function that computes leading zeros.
 *
 * @param[in] bset - binary values of a given bitset
 * @returns leading zeros of given binary set
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  /** @TODO(student) Implement this function! */
  for (uint64_t i = inital_bits_; i < BITSET_CAPACITY; i++) {
    uint64_t j = BITSET_CAPACITY - i - 1;
    // LOG_DEBUG("Position: %lu, Bit: %d", (unsigned long)i, bset.test(j));
    if (bset.test(j) ) {
      return i-inital_bits_ + 1;
    }
  }
  return 1;
}

/**
 * @brief Adds a value into the HyperLogLog.
 *
 * @param[in] val - value that's added into hyperloglog
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  hash_t hash = CalculateHash(val);
  LOG_DEBUG("Hash value: %lu", (unsigned long)hash);
  // compute the index
  uint64_t index = (hash>>(BITSET_CAPACITY-inital_bits_)) & ((1 << inital_bits_) - 1);
  LOG_DEBUG("Index: %lu", (unsigned long)index);
  // compute the leading ones
  std::bitset<BITSET_CAPACITY> bset = ComputeBinary(hash);
  LOG_DEBUG("Binary: %s", bset.to_string().c_str());

  uint64_t leading_ones = PositionOfLeftmostOne(bset);
  LOG_DEBUG("Leading ones: %lu", (unsigned long)leading_ones);
  // update the bucket
  if (leading_ones > dense_bucket_[index]) {
    dense_bucket_[index] = leading_ones;
  }

}

/**
 * @brief Function that computes cardinality.
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  uint64_t m = dense_bucket_.size();//reserve the last one for index
  double sum = 0;
  for (uint64_t i = 0; i < m; i++) {
    sum += std::pow(2, -static_cast<int64_t>(dense_bucket_[i]));
  }
  LOG_DEBUG("Sum: %lf, m: %lu", sum, (unsigned long)m);
  double cardinality = CONSTANT * m * m / sum;
  LOG_DEBUG("Cardinality: %lf", cardinality);
  cardinality_ = std::floor(cardinality);
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub
