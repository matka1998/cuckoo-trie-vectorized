#ifndef INTRINSICS_H
#define INTRINSICS_H

#include "cuckoo_trie_internal.h"
int find_free_cell_by_mask(ct_bucket* bucket, uint32_t mask, uint32_t values);

#endif // INTRINSICS_H