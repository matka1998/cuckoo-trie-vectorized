#include "cuckoo_trie_internal.h"
#include <immintrin.h>

inline __m128i load_all_common_headers_in_bucket(ct_bucket* bucket) {
    // The common header is 3 bytes per cell, 4 cells -> 12 bytes total per bucket
    // Load atomically as 64-bit + 32-bit, then expand to four 32-bit lanes
    uint64_t* bucket_headers_first_part = (uint64_t*)&bucket->common_cells[0];
    uint32_t* bucket_headers_second_part = (uint32_t*)(((uint64_t*)&bucket->common_cells[0]) + 1);

    const uint64_t first8 = __atomic_load_n(bucket_headers_first_part, __ATOMIC_ACQUIRE);
    const uint32_t next4  = __atomic_load_n(bucket_headers_second_part, __ATOMIC_ACQUIRE);

    // Build a 128-bit vector of the 12 bytes (upper 4 bytes zero)
    __m128i bytes128 = _mm_setzero_si128();
    bytes128 = _mm_insert_epi64(bytes128, (long long)first8, 0);
    bytes128 = _mm_insert_epi32(bytes128, (int)next4, 2); // place at byte offset 8..11

    // Shuffle mask to expand 3 bytes -> 4 bytes per lane with zero in the MSB
    const __m128i shuffle = _mm_setr_epi8(
        0, 1, 2, (char)0x80,
        3, 4, 5, (char)0x80,
        6, 7, 8, (char)0x80,
        9,10,11, (char)0x80
    );
    return _mm_shuffle_epi8(bytes128, shuffle);
}

inline __m256i load_both_bucket_common_headers(ct_bucket * primary_bucket, ct_bucket * secondary_bucket) {
    __m128i primary_data = load_all_common_headers_in_bucket(primary_bucket);
    __m128i secondary_data = load_all_common_headers_in_bucket(secondary_bucket);

    return _mm256_setr_m128i(primary_data, secondary_data);
}

int find_free_cell_by_mask(ct_bucket* primary_bucket, ct_bucket* secondary_bucket, uint32_t mask, uint32_t primary_values, uint32_t secondary_values) {
    __m256i headers = load_both_bucket_common_headers(primary_bucket, secondary_bucket);

    // TODO: Make sure in disassembly that we do in fact output vpbroadcastd and not an instruction sequence.
    __m256i mask_vec = _mm256_set1_epi32(mask);

    // TODO: Try to alternatively call set1 for the 256 bit register variation and combine using blend. Blend is more efficient that way,
    // but the set is worse for 256 bit - try to experiment what works better.
    __m128i primary_values_vec = _mm_set1_epi32(primary_values);
    __m128i secondary_values_vec = _mm_set1_epi32(secondary_values);
    __m256i combined_values_vec = _mm256_setr_m128i(primary_values_vec, secondary_values_vec);

    __mmask8 result = _mm256_cmp_epi32_mask(_mm256_and_si256(headers, mask_vec), combined_values_vec, _MM_CMPINT_EQ);

    if (result == 0)
        return -1;

    return __builtin_ctz(result);
}