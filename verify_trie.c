#include <stdio.h>
#include <assert.h>
#include <stdlib.h>

#include "main.h"
#include "util.h"

#define MAX_KEY_BYTES_TO_PRINT 20

static void print_key(ct_kv* kv) {
	int i;

	for (i = 0;i < kv_key_size(kv);i++) {
		printf("%02x ", kv_key_bytes(kv)[i]);

		if (i == MAX_KEY_BYTES_TO_PRINT) {
			printf("...");
			break;
		}
	}
}

// Similar to locator_to_entry, but the locator may point to nothing (locator_to_entry
// enters an infinite loop in this case).
ct_entry_storage* try_follow_locator(cuckoo_trie* trie, ct_entry_locator* locator) {
	ct_entry_storage* result;
	ct_entry_local_copy unused;

	//TODO: can enter infinite loop if locator not found?
	result = find_entry_in_pair_by_color(trie, &unused, locator->primary_bucket,
										 locator->tag, locator->color);
	assert(result);
	return result;
}

ct_entry_descriptor try_follow_locator_split_view(cuckoo_trie_split* trie, ct_entry_locator* locator) {
	ct_entry_descriptor result = {0};
	ct_entry_local_copy_split unused;

	//TODO: can enter infinite loop if locator not found?
	result = find_entry_in_pair_by_color_split_view(trie, &unused, locator->primary_bucket,
										 locator->tag, locator->color);
	assert(result.common_header);
	return result;
}

char* type_name(int type) {
	switch (type) {
		case TYPE_BITMAP: return "BITMAP";
		case TYPE_JUMP: return "JUMP";
		case TYPE_LEAF: return "LEAF";
		case TYPE_UNUSED: return "UNUSED";
		default: return "UNKNOWN";
	}
}

char* leaf_locator_error(cuckoo_trie* trie, ct_entry_locator* locator) {
#ifdef NO_LINKED_LIST
	return 0;
#else
	ct_entry_storage* pointed_entry;
	if (locator->primary_bucket >= trie->num_buckets)
		return "BUCKET_TOO_LARGE";

	pointed_entry = try_follow_locator(trie, locator);
	if (!pointed_entry)
		return "POINTS_TO_NOTHING";

	if (entry_type((ct_entry*) pointed_entry) != TYPE_LEAF)
		return "POINTS_TO_NON_LEAF";

	return 0;
#endif
}


char* leaf_locator_error_split_view(cuckoo_trie_split* trie, ct_entry_locator* locator) {
#ifdef NO_LINKED_LIST
	return 0;
#else
	ct_entry_descriptor pointed_entry;
	if (locator->primary_bucket >= trie->num_buckets)
		return "BUCKET_TOO_LARGE";

	pointed_entry = try_follow_locator_split_view(trie, locator);
	if (!pointed_entry.common_header)
		return "POINTS_TO_NOTHING";

	if (entry_type_common_header((ct_common_header*) pointed_entry.common_header) != TYPE_LEAF)
		return "POINTS_TO_NON_LEAF";

	return 0;
#endif
}

void print_locator(ct_entry_locator* locator) {
	printf("\tLocator: primary_bucket=%u tag=%x color=%d\n",
				   locator->primary_bucket, locator->tag,
				   locator->color);
}

void print_entry(cuckoo_trie* trie, uint64_t bucket, int cell) {
	ct_entry* entry = (ct_entry*) &(trie->buckets[bucket].cells[cell]);
	printf("\tBucket %lu cell %d (%p): %s tag=0x%x last_symbol=0x%x\n",
					   bucket, cell, entry, type_name(entry_type(entry)), entry_tag(entry),
					   entry->last_symbol);
}


void print_entry_split_view(cuckoo_trie_split* trie, uint64_t bucket, int cell) {
	ct_common_header* entry = (ct_common_header*) &(trie->buckets[bucket].common_header_cells[cell]);
	printf("\tBucket %lu cell %d (%p): %s tag=0x%x last_symbol=0x%x\n",
					   bucket, cell, entry, type_name(entry_type_common_header(entry)), entry_tag_common_header(entry),
					   entry->last_symbol);
}

int verify_bitmap_children(cuckoo_trie* trie, uint64_t bucket, int cell) {
	int child;
	ct_entry* entry = (ct_entry*) &(trie->buckets[bucket].cells[cell]);
	uint64_t primary_bucket = bucket;
	uint64_t prefix_hash;

	if (entry_is_secondary(entry))
		primary_bucket = unmix_bucket(trie, bucket, entry_tag(entry));

	prefix_hash = (primary_bucket << TAG_BITS) + entry_tag(entry);

	for (child = 0;child < FANOUT + 1;child++) {
		if (!get_bit(entry->child_bitmap, child))
			continue;

		uint64_t child_prefix_hash = accumulate_hash(trie, prefix_hash, child);
		ct_entry_local_copy unused;
		ct_entry_storage* result = find_entry_in_pair_by_parent(trie, &unused,
																hash_to_bucket(child_prefix_hash),
																hash_to_tag(child_prefix_hash),
																child,
																entry_color(entry));
		if (!result) {
			printf("Error: bitmap claims to have child %x, but it doesn't exist\n", child);
			return 0;
		}
	}
	return 1;
}


int verify_bitmap_children_split_view(cuckoo_trie_split* trie, uint64_t bucket, int cell) {
	int child;
	// ct_entry* entry = (ct_entry*) &(trie->buckets[bucket].cells[cell]);
	ct_common_header* entry_common = (ct_common_header*) &(trie->buckets[bucket].common_header_cells[cell]);
	ct_type_specific_entry* entry_type_specific = (ct_type_specific_entry*) &(trie->buckets[bucket].type_specific_cells[cell]);
	uint64_t primary_bucket = bucket;
	uint64_t prefix_hash;

	if (entry_is_secondary_common_header(entry_common))
		primary_bucket = unmix_bucket_split_view(trie, bucket, entry_tag_common_header(entry_common));

	prefix_hash = (primary_bucket << TAG_BITS) + entry_tag_common_header(entry_common);

	for (child = 0;child < FANOUT + 1;child++) {
		if (!get_bit(entry_type_specific->child_bitmap, child))
			continue;

		uint64_t child_prefix_hash = accumulate_hash_split_view(trie, prefix_hash, child);
		ct_entry_local_copy_split unused;
		ct_entry_descriptor result = find_entry_in_pair_by_parent_split_view(trie, &unused,
																hash_to_bucket(child_prefix_hash),
																hash_to_tag(child_prefix_hash),
																child,
																entry_color_common_header(entry_common));
		if (!result.common_header) {
			printf("Error: bitmap claims to have child %x, but it doesn't exist\n", child);
			return 0;
		}
	}
	return 1;
}

int verify_jump_child(cuckoo_trie* trie, uint64_t bucket, int cell) {
	int i;
	ct_entry_storage* child;
	ct_entry_local_copy unused;
	ct_entry* entry = (ct_entry*) &(trie->buckets[bucket].cells[cell]);
	uint64_t primary_bucket = bucket;
	uint64_t prefix_hash;

	if (entry_is_secondary(entry))
		primary_bucket = unmix_bucket(trie, bucket, entry_tag(entry));

	prefix_hash = (primary_bucket << TAG_BITS) + entry_tag(entry);

	for (i = 0;i < entry_jump_size(entry);i++)
		prefix_hash = accumulate_hash(trie, prefix_hash, get_jump_symbol(entry, i));

	child = find_entry_in_pair_by_color(trie, &unused, hash_to_bucket(prefix_hash),
										hash_to_tag(prefix_hash), entry_child_color(entry));
	if (!child) {
		printf("Error: Jump child doesn't exist\n");
		return 0;
	}

	if (entry_type((ct_entry*) child) == TYPE_LEAF) {
		printf("Error: Jump child is leaf\n");
		return 0;
	}

	return 1;
}

int verify_jump_child_split_view(cuckoo_trie_split* trie, uint64_t bucket, int cell) {
	int i;
	// ct_entry_storage* child;
	ct_entry_descriptor child = {0};
	ct_entry_local_copy_split unused;
	ct_common_header* entry_common = (ct_common_header*) &(trie->buckets[bucket].common_header_cells[cell]);
	ct_type_specific_entry* entry_type_specific = (ct_type_specific_entry*) &(trie->buckets[bucket].type_specific_cells[cell]);
	uint64_t primary_bucket = bucket;
	uint64_t prefix_hash;

	if (entry_is_secondary_common_header(entry_common))
		primary_bucket = unmix_bucket_split_view(trie, bucket, entry_tag_common_header(entry_common));

	prefix_hash = (primary_bucket << TAG_BITS) + entry_tag_common_header(entry_common);

	for (i = 0;i < entry_jump_size_type_specific(entry_type_specific);i++)
		prefix_hash = accumulate_hash_split_view(trie, prefix_hash, get_jump_symbol_type_specific(entry_type_specific, i));

	child = find_entry_in_pair_by_color_split_view(trie, &unused, hash_to_bucket(prefix_hash),
										hash_to_tag(prefix_hash), entry_child_color_type_specific(entry_type_specific));
	if (!child.common_header) {
		printf("Error: Jump child doesn't exist\n");
		return 0;
	}

	if (entry_type_common_header((ct_common_header*) child.common_header) == TYPE_LEAF) {
		printf("Error: Jump child is leaf\n");
		return 0;
	}

	return 1;
}


// Should only be called by the writer thread
int verify_entry(cuckoo_trie* trie, uint64_t bucket, int cell) {
	int is_ok = 1;
	ct_entry* entry = (ct_entry*) &(trie->buckets[bucket].cells[cell]);

	if (entry_type(entry) != TYPE_LEAF) {
		// This entry has a max_leaf field. Verify it.
		char* max_leaf_error = leaf_locator_error(trie, &(entry->max_leaf));
		if (max_leaf_error != NULL) {
			printf("Error: max_leaf locator of bucket %lu cell %d is broken (%s).\n",
				   bucket, cell, max_leaf_error);
			print_locator(&(entry->max_leaf));
			is_ok = 0;
		}
	}

	if (entry_type(entry) == TYPE_BITMAP) {
		if (verify_bitmap_children(trie, bucket, cell) == 0)
			is_ok = 0;
	}
	if (entry_type(entry) == TYPE_JUMP) {
		if (verify_jump_child(trie, bucket, cell) == 0)
			is_ok = 0;
	}
	if (entry_type(entry) == TYPE_LEAF) {
		int is_max_leaf = (entry->next_leaf.primary_bucket == ((uint32_t)-1));
		if (!is_max_leaf) {
			char* next_leaf_error = leaf_locator_error(trie, &(entry->next_leaf));
			if (next_leaf_error != NULL) {
				printf("Error: next_leaf locator of bucket %lu cell %d is broken (%s).\n",
					   bucket, cell, next_leaf_error);
				print_locator(&(entry->next_leaf));
				is_ok = 0;
			}
		}
	}


	return is_ok;
}


// Should only be called by the writer thread
int verify_entry_split_view(cuckoo_trie_split* trie, uint64_t bucket, int cell) {
	int is_ok = 1;
	// ct_entry* entry = (ct_entry*) &(trie->buckets[bucket].cells[cell]);
	ct_common_header * entry_common = (ct_common_header*) &(trie->buckets[bucket].common_header_cells[cell]);
	ct_type_specific_entry * entry_type_specific = (ct_type_specific_entry*) &(trie->buckets[bucket].type_specific_cells[cell]);

	if (entry_type_common_header(entry_common) != TYPE_LEAF) {
		// This entry has a max_leaf field. Verify it.
		char* max_leaf_error = leaf_locator_error_split_view(trie, &(entry_type_specific->max_leaf));
		if (max_leaf_error != NULL) {
			printf("Error: max_leaf locator of bucket %lu cell %d is broken (%s).\n",
				   bucket, cell, max_leaf_error);
			print_locator(&(entry_type_specific->max_leaf));
			is_ok = 0;
		}
	}

	if (entry_type_common_header(entry_common) == TYPE_BITMAP) {
		if (verify_bitmap_children_split_view(trie, bucket, cell) == 0)
			is_ok = 0;
	}
	if (entry_type_common_header(entry_common) == TYPE_JUMP) {
		if (verify_jump_child_split_view(trie, bucket, cell) == 0)
			is_ok = 0;
	}
	if (entry_type_common_header(entry_common) == TYPE_LEAF) {
		int is_max_leaf = (entry_type_specific->next_leaf.primary_bucket == ((uint32_t)-1));
		if (!is_max_leaf) {
			char* next_leaf_error = leaf_locator_error_split_view(trie, &(entry_type_specific->next_leaf));
			if (next_leaf_error != NULL) {
				printf("Error: next_leaf locator of bucket %lu cell %d is broken (%s).\n",
					   bucket, cell, next_leaf_error);
				print_locator(&(entry_type_specific->next_leaf));
				is_ok = 0;
			}
		}
	}


	return is_ok;
}

int verify_linklist(cuckoo_trie* trie) {
	int cell;
	int leaf_cell;
	int is_ok = 1;
	uint64_t bucket;
	uint64_t leaf_bucket;
	uint64_t linklist_leaves = 0;
	uint64_t num_leaves = 0;
	uint8_t* unlinked_leaves = calloc(trie->num_buckets * CUCKOO_BUCKET_SIZE, 1);
	ct_kv* last_kv = NULL;
	ct_entry_locator next = ((ct_entry*) trie_min_leaf(trie))->next_leaf;

	for (bucket = 0; bucket < trie->num_buckets; bucket++) {
		for (cell = 0; cell < CUCKOO_BUCKET_SIZE; cell++) {
			if (entry_type((ct_entry*) &(trie->buckets[bucket].cells[cell])) == TYPE_LEAF) {
				unlinked_leaves[bucket * CUCKOO_BUCKET_SIZE + cell] = 1;
				num_leaves++;
			}
		}
	}

	while (next.primary_bucket != (uint32_t)-1) {
		ct_entry_storage* leaf = try_follow_locator(trie, &next);
		if (!leaf) {
			printf("Error: Reached a next_leaf locator that doesn't point anywhere.\n");
			print_locator(&next);
			is_ok = 0;
			goto ret;
		}
		if (entry_type((ct_entry*) leaf) != TYPE_LEAF) {
			printf("Error: Reached a next_leaf locator that doesn't point to a leaf.\n");
			print_locator(&next);
			is_ok = 0;
			goto ret;
		}
		if (last_kv != NULL) {
			if (kv_key_compare(entry_kv((ct_entry*) leaf), last_kv) <= 0) {
				printf("Error: Leaf %p key is smaller than previous\n", leaf);
				printf("\tKey of leaf:     ");
				print_key(entry_kv((ct_entry*) leaf));
				printf("\n");
				printf("\tKey of previous: ");
				print_key(last_kv);
				printf("\n");
				is_ok = 0;
				goto ret;
			}
		}

		leaf_bucket = ptr_to_bucket(trie, leaf);
		leaf_cell = entry_index_in_bucket(leaf);
		unlinked_leaves[leaf_bucket * CUCKOO_BUCKET_SIZE + leaf_cell] = 0;

		last_kv = entry_kv((ct_entry*) leaf);
		linklist_leaves++;
		if (linklist_leaves > num_leaves) {
			printf("Error: next_leaf linked-list entered a loop.\n");
			is_ok = 0;
			goto ret;
		}

		next = ((ct_entry*) leaf)->next_leaf;

	}

	for (bucket = 0; bucket < trie->num_buckets; bucket++) {
		for (cell = 0; cell < CUCKOO_BUCKET_SIZE; cell++) {
			if (unlinked_leaves[bucket * CUCKOO_BUCKET_SIZE + cell] == 1) {
				printf("Unlinked leaf\n");
				print_entry(trie, bucket, cell);
				is_ok = 0;
			}
		}
	}

ret:
	free(unlinked_leaves);
	return is_ok;
}


int verify_linklist_split_view(cuckoo_trie_split* trie) {
	int cell;
	int leaf_cell;
	int is_ok = 1;
	uint64_t bucket;
	uint64_t leaf_bucket;
	uint64_t linklist_leaves = 0;
	uint64_t num_leaves = 0;
	uint8_t* unlinked_leaves = calloc(trie->num_buckets * CUCKOO_BUCKET_SIZE, 1);
	ct_kv* last_kv = NULL;
	ct_entry_locator next = ((ct_type_specific_entry*) trie_min_leaf_split_view(trie).type_specific)->next_leaf;

	for (bucket = 0; bucket < trie->num_buckets; bucket++) {
		for (cell = 0; cell < CUCKOO_BUCKET_SIZE; cell++) {
			if (entry_type_common_header((ct_common_header*) &(trie->buckets[bucket].common_header_cells[cell])) == TYPE_LEAF) {
				unlinked_leaves[bucket * CUCKOO_BUCKET_SIZE + cell] = 1;
				num_leaves++;
			}
		}
	}

	while (next.primary_bucket != (uint32_t)-1) {
		ct_entry_descriptor leaf = try_follow_locator_split_view(trie, &next);
		if (!leaf.common_header) {
			printf("Error: Reached a next_leaf locator that doesn't point anywhere.\n");
			print_locator(&next);
			is_ok = 0;
			goto ret;
		}
		if (entry_type_common_header((ct_common_header*) leaf.common_header) != TYPE_LEAF) {
			printf("Error: Reached a next_leaf locator that doesn't point to a leaf.\n");
			print_locator(&next);
			is_ok = 0;
			goto ret;
		}
		if (last_kv != NULL) {
			if (kv_key_compare(entry_kv_type_specific((ct_type_specific_entry*) leaf.type_specific), last_kv) <= 0) {
				printf("Error: Leaf %p key is smaller than previous\n", leaf.common_header);
				printf("\tKey of leaf:     ");
				print_key(entry_kv_type_specific((ct_type_specific_entry*) leaf.type_specific));
				printf("\n");
				printf("\tKey of previous: ");
				print_key(last_kv);
				printf("\n");
				is_ok = 0;
				goto ret;
			}
		}

		leaf_bucket = ptr_to_bucket_common_header(trie, leaf.common_header);
		leaf_cell = entry_index_in_bucket_common_header(leaf.common_header);
		unlinked_leaves[leaf_bucket * CUCKOO_BUCKET_SIZE + leaf_cell] = 0;

		last_kv = entry_kv_type_specific((ct_type_specific_entry*) leaf.type_specific);
		linklist_leaves++;
		if (linklist_leaves > num_leaves) {
			printf("Error: next_leaf linked-list entered a loop.\n");
			is_ok = 0;
			goto ret;
		}

		next = ((ct_type_specific_entry*) leaf.type_specific)->next_leaf;
	}

	for (bucket = 0; bucket < trie->num_buckets; bucket++) {
		for (cell = 0; cell < CUCKOO_BUCKET_SIZE; cell++) {
			if (unlinked_leaves[bucket * CUCKOO_BUCKET_SIZE + cell] == 1) {
				printf("Unlinked leaf\n");
				print_entry_split_view(trie, bucket, cell);
				is_ok = 0;
			}
		}
	}

ret:
	free(unlinked_leaves);
	return is_ok;
}

int ct_verify_trie(cuckoo_trie* trie) {
	int cell;
	uint64_t bucket;
	int is_ok = 1;

	for (bucket = 0; bucket < trie->num_buckets; bucket++) {
		if (trie->buckets[bucket].write_lock) {
			printf("Error: Bucket %lu left write-locked\n", bucket);
			is_ok = 0;
		}
		for (cell = 0; cell < CUCKOO_BUCKET_SIZE; cell++) {
			ct_entry* entry = (ct_entry*) &(trie->buckets[bucket].cells[cell]);
			if (entry_type(entry) == TYPE_UNUSED)
				continue;
			if (!verify_entry(trie, bucket, cell)) {
				print_entry(trie, bucket, cell);
				is_ok = 0;
			}
		}
	}

#ifndef NO_LINKED_LIST
	if (!verify_linklist(trie))
		is_ok = 0;
#endif

	return is_ok;
}


int ct_verify_trie_split_view(cuckoo_trie_split* trie) {
	int cell;
	uint64_t bucket;
	int is_ok = 1;

	for (bucket = 0; bucket < trie->num_buckets; bucket++) {
		if (trie->buckets[bucket].write_lock) {
			printf("Error: Bucket %lu left write-locked\n", bucket);
			is_ok = 0;
		}
		for (cell = 0; cell < CUCKOO_BUCKET_SIZE; cell++) {
			ct_common_header* entry = (ct_common_header*) &(trie->buckets[bucket].common_header_cells[cell]);
			if (entry_type_common_header(entry) == TYPE_UNUSED)
				continue;
			if (!verify_entry_split_view(trie, bucket, cell)) {
				print_entry_split_view(trie, bucket, cell);
				is_ok = 0;
			}
		}
	}

#ifndef NO_LINKED_LIST
	if (!verify_linklist_split_view(trie))
		is_ok = 0;
#endif

	return is_ok;
}

