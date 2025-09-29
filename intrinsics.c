#include "cuckoo_trie_internal.h"
#include <immintrin.h>


// Assumes CUCKOO_BUCKET_SIZE == 4
static inline __m256i load_both_bucket_common_headers(ct_bucket* primary_bucket,
                                                      ct_bucket* secondary_bucket) {
    assert(CUCKOO_BUCKET_SIZE == 4);
    // Prefetch buckets to reduce latency
    _mm_prefetch((const char*)primary_bucket, _MM_HINT_T0);
    _mm_prefetch((const char*)secondary_bucket, _MM_HINT_T0);

    // Load 8 dwords (4 from each bucket) with acquire semantics
    const uint32_t p0 = __atomic_load_n((const uint32_t*)&primary_bucket->cells[0], __ATOMIC_RELAXED);
    const uint32_t p1 = __atomic_load_n((const uint32_t*)&primary_bucket->cells[1], __ATOMIC_RELAXED);
    const uint32_t p2 = __atomic_load_n((const uint32_t*)&primary_bucket->cells[2], __ATOMIC_RELAXED);
    const uint32_t p3 = __atomic_load_n((const uint32_t*)&primary_bucket->cells[3], __ATOMIC_RELAXED);

    const uint32_t s0 = __atomic_load_n((const uint32_t*)&secondary_bucket->cells[0], __ATOMIC_RELAXED);
    const uint32_t s1 = __atomic_load_n((const uint32_t*)&secondary_bucket->cells[1], __ATOMIC_RELAXED);
    const uint32_t s2 = __atomic_load_n((const uint32_t*)&secondary_bucket->cells[2], __ATOMIC_RELAXED);
    const uint32_t s3 = __atomic_load_n((const uint32_t*)&secondary_bucket->cells[3], __ATOMIC_RELAXED);

    const __m128i primary_values_vec = _mm_setr_epi32((int)p0, (int)p1, (int)p2, (int)p3);
    const __m128i secondary_values_vec = _mm_setr_epi32((int)s0, (int)s1, (int)s2, (int)s3);

    return _mm256_inserti128_si256(_mm256_castsi128_si256(primary_values_vec),
                                   secondary_values_vec,
                                   1);
}

int find_free_cell_by_mask(ct_bucket* primary_bucket, ct_bucket* secondary_bucket,
                           uint32_t mask, uint32_t primary_values, uint32_t secondary_values) {
    __m256i headers = load_both_bucket_common_headers(primary_bucket, secondary_bucket);

    // Apply the 24-bit header constraint once together with the provided mask
    const uint32_t combined_mask = mask & 0x00FFFFFFu;
    __m256i mask_vec = _mm256_set1_epi32((int)combined_mask);

    __m128i primary_values_vec = _mm_set1_epi32((int)primary_values);
    __m128i secondary_values_vec = _mm_set1_epi32((int)secondary_values);
    __m256i combined_values_vec = _mm256_inserti128_si256(
                                      _mm256_castsi128_si256(primary_values_vec),
                                      secondary_values_vec,
                                      1);

    __mmask8 result = _mm256_cmp_epi32_mask(_mm256_and_si256(headers, mask_vec),
                                            combined_values_vec,
                                            _MM_CMPINT_EQ);

    if (result == 0)
        return -1;

    return __builtin_ctz(result);
}