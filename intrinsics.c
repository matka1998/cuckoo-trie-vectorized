#include "cuckoo_trie_internal.h"
#include <immintrin.h>

inline __m128i load_all_common_headers_in_bucket(ct_bucket* bucket) {
    __mmask16 load_mask = 0b1110111011101110;

    assert((sizeof(ct_common_header_storage) * CUCKOO_BUCKET_SIZE) == 12);

    ct_common_header_storage headers[CUCKOO_BUCKET_SIZE];
    // 3 bytes for each common header, 4 headers per bucket totals 12 bytes, or 1 qword + 1 dword.
    uint64_t * local_headers_first_part = (uint64_t*) &headers[0];
    uint32_t * local_headers_second_part = (uint32_t*) &headers[0] + 8;
    uint64_t * bucket_headers_first_part = (uint64_t*) &bucket->common_cells[0];
    uint32_t * bucket_headers_second_part = (uint32_t*) (((uint8_t*)&bucket->common_cells[0]) + 8);

    // first atomically load the entire bucket.
    *local_headers_first_part = __atomic_load_n(bucket_headers_first_part, __ATOMIC_ACQUIRE);
    *local_headers_second_part = __atomic_load_n(bucket_headers_second_part, __ATOMIC_ACQUIRE);

    // Then read and expand.
    return _mm_maskz_expandloadu_epi8(load_mask, &headers[0]);
}

int find_free_cell_by_mask(ct_bucket* bucket, uint32_t mask, uint32_t values) {
    __m128i headers = load_all_common_headers_in_bucket(bucket);

    __m128i mask_vec = _mm_set1_epi32(mask);
    __m128i values_vec = _mm_set1_epi32(values);

    __mmask8 result = _mm_cmp_epi32_mask(_mm_and_si128(headers, mask_vec), values_vec, _MM_CMPINT_EQ);

    if (result == 0)
        return -1;
    return __builtin_ctz(result);
}