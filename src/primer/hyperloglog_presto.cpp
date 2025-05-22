//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog_presto.cpp
//
// Identification: src/primer/hyperloglog_presto.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/hyperloglog_presto.h"
#include <bitset>
#include <climits>
#include <cmath>
#include "common/logger.h"

namespace bustub {

/** @brief Parameterized constructor. */
template <typename KeyType>
HyperLogLogPresto<KeyType>::HyperLogLogPresto(int16_t n_leading_bits) : cardinality_(0),inital_bits_(n_leading_bits) {
  if (n_leading_bits<0) {
    inital_bits_ = 0;
  }
  // LOG_INFO("HyperLogLog initialized with %lu bits", static_cast<unsigned long>(inital_bits_));
  dense_bucket_.resize(1 << inital_bits_); // size = 2^n_leading_bits
}

/** @brief Element is added for HLL calculation. */
template <typename KeyType>
auto HyperLogLogPresto<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  // first, calculate the hash of the value
  hash_t hash = CalculateHash(val);
  // then, convert the hash to binary
  constexpr uint64_t hash_size = sizeof(hash_t) * CHAR_BIT;
  std::bitset<hash_size> bset(hash);
  // LOG_DEBUG("Hash: %lu, Binary: %s", (unsigned long)hash, bset.to_string().c_str());
  // find the position of the rightmost contiguous set of zeros 
  uint64_t bucket_value = 0;
  for (uint64_t i = 0; i < hash_size-inital_bits_; i++) {
    if (bset.test(i)) {
      break;
    }
    bucket_value++;
  }
  uint64_t bucket_index = (hash>>(hash_size-inital_bits_)) & ((1 << inital_bits_) - 1);
  // LOG_DEBUG("Bucket Index: %lu, Bucket Value: %lu", (unsigned long)bucket_index, (unsigned long)bucket_value);
  InsertToBucket(bucket_index, bucket_value);
}

/** @brief Function to compute cardinality. */
template <typename T>
auto HyperLogLogPresto<T>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  // compute the cardinality
  double sum = 0;
  uint64_t m = 1 << inital_bits_;
  for (uint64_t i = 0; i < m; i++) {
    // get the number of leading zeros
    sum+=pow(2, -static_cast<int64_t>(GetFromBucket(i)));
  }
  double alpha_mm = CONSTANT * m * m / sum;
  cardinality_ = static_cast<uint64_t>(std::floor(alpha_mm));
}

template class HyperLogLogPresto<int64_t>;
template class HyperLogLogPresto<std::string>;
}  // namespace bustub
