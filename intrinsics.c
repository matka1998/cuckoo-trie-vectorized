#include "cuckoo_trie_internal.h"
#include <immintrin.h>


inline __m128i load_all_common_headers_in_bucket(ct_bucket* bucket) {
    __mmask16 load_mask = 0b0111011101110111;

    uint8_t headers[CUCKOO_BUCKET_SIZE*3];
    // 3 bytes for each common header, 4 headers per bucket totals 12 bytes, or 1 qword + 1 dword.
    memcpy(headers, &bucket->cells[0], 3);
    memcpy(headers+3, &bucket->cells[1], 3);
    memcpy(headers+6, &bucket->cells[2], 3);
    memcpy(headers+9, &bucket->cells[3], 3);

    // Then read and expand.
    return _mm_maskz_expandloadu_epi8(load_mask, headers);
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