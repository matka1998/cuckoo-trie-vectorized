#ifndef INTRINSICS_H
#define INTRINSICS_H

#include "cuckoo_trie_internal.h"
int find_free_cell_by_mask(ct_bucket* primary_bucket, ct_bucket* secondary_bucket, uint32_t mask, uint32_t primary_values, uint32_t secondary_values);

#endif // INTRINSICS_H