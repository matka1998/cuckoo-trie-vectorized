#include <assert.h>
#include <stddef.h>
#include "cuckoo_trie.h"
#include "config.h"
#include "compiler.h"

#if TAG_BITS > 5
#error "Cannot store tag and color in one byte"
#endif

#define NUM_SEQLOCK_COUNTERS (1 << LOG_SEQLOCK_COUNTERS)
#define FANOUT (1 << BITS_PER_SYMBOL)
#define CHILD_BITMAP_BYTES ((FANOUT + 1 + 7) / 8)    // +1 for the END symbol
#define MAX_JUMP_BITS (MAX_JUMP_SYMBOLS * BITS_PER_SYMBOL)
#define SYMBOL_END 0
#define MAX_KEY_SYMBOLS ((MAX_KEY_BYTES * 8 + BITS_PER_SYMBOL - 1) / BITS_PER_SYMBOL)
#define SYMBOL_MASK (FANOUT - 1)
#define SYMBOL_ABOVE_MAX (FANOUT + 1)

#define NUM_LINKED_LISTS 1

#define CACHELINE_BYTES 64

#define TYPE_UNUSED 0
#define TYPE_LEAF 1
#define TYPE_BITMAP 2
#define TYPE_JUMP 3

#define TYPE_MASK 3
#define FLAG_SECONDARY_BUCKET 4

#ifdef MULTITHREADING
#define FLAG_DIRTY 8
#endif

#define PARENT_COLOR_SHIFT 4
#define CHILD_COLOR_SHIFT 4

typedef struct {
	uint32_t primary_bucket;
	uint8_t tag;
	uint8_t color;
} __attribute__((packed)) ct_entry_locator;

// In x86_64, the most significant 16 bits of a pointer are identical
// to the 17th bit. Therefore, we can store a pointer using just 48 bits.
typedef struct {
	uint32_t low_dword;
	uint16_t high_word;
} __attribute__((packed)) ct_small_pointer;

typedef union {
	struct {
		uint8_t parent_color_and_flags;
		uint8_t color_and_tag;
		uint8_t last_symbol;
		uint8_t _padding_start[0];
	};
	uint8_t pad[3];

} ct_common_header;

typedef union {
	struct {
		union {
			// Bitmap or jump node
			struct {
				union {
					// Bitmap node
					struct {
						uint8_t child_bitmap[CHILD_BITMAP_BYTES];
						uint8_t max_child;
					};

					// Jump node
					struct {
						uint8_t jump_bits[(MAX_JUMP_BITS + 7) / 8];
						uint8_t child_color_and_jump_size;
					};
				};

				ct_entry_locator max_leaf;
			} __attribute__((packed));

			// Leaf node
			struct {
				ct_small_pointer key;
				ct_entry_locator next_leaf;
			} __attribute__((packed));
		} __attribute__((packed));

		uint8_t _padding_start[0];
	};

	uint8_t pad[12];
} ct_type_specific_entry;

// This type is used for local variables containing entries. It is padded to 16 bytes
// to allow reading/writing it as two QWORDs with no overlap (overlapping QWORDs have
// a performance penalty due to store-forwarding issues).
// The entries in the buckets use the non-padded type ct_entry_storage to conserve space.
typedef union {
	struct {
		// Common header
		uint8_t parent_color_and_flags;
		uint8_t color_and_tag;
		uint8_t last_symbol;

		// Type-specific part
		union {
			// Bitmap or jump node
			struct {
				union {
					// Bitmap node
					struct {
						uint8_t child_bitmap[CHILD_BITMAP_BYTES];
						uint8_t max_child;
					};

					// Jump node
					struct {
						uint8_t jump_bits[(MAX_JUMP_BITS + 7) / 8];
						uint8_t child_color_and_jump_size;
					};
				};

				ct_entry_locator max_leaf;
			} __attribute__((packed));

			// Leaf node
			struct {
				ct_small_pointer key;
				ct_entry_locator next_leaf;
			} __attribute__((packed));
		} __attribute__((packed));

		uint8_t _padding_start[0];
	};

	uint8_t pad[16];
} ct_entry;

typedef struct {
	uint8_t bytes[offsetof(ct_entry, _padding_start)];
} ct_entry_storage;

typedef struct {
	uint8_t bytes[offsetof(ct_common_header, _padding_start)];
} ct_common_header_storage;

typedef struct {
	uint8_t bytes[offsetof(ct_type_specific_entry, _padding_start)];
} ct_type_specific_entry_storage;

typedef struct {
	ct_common_header_storage * common_header;
	ct_type_specific_entry_storage * type_specific;
} ct_entry_descriptor;

// Used to allow updating an entry after we read it. The entry might have been
// relocated since it was read, so we save the required information to find
// it again.
typedef struct {
	ct_entry value;

	// Where the entry was read from
	ct_entry_storage* last_pos;

	// Used to find the entry again, in case it was relocated from last_pos
	uint64_t primary_bucket;

	// The sequence of the node's bucket when it was read
	uint64_t last_seq;
} ct_entry_local_copy;

typedef struct {
	ct_common_header header;
	ct_type_specific_entry type_specific;

	ct_common_header_storage * last_common_pos;
	ct_type_specific_entry_storage * last_type_specific_pos;

	uint64_t primary_bucket;

	uint64_t last_seq;
} ct_entry_local_copy_split;

typedef union {
	struct {
		ct_entry_storage cells[CUCKOO_BUCKET_SIZE];
		union {
			uint32_t write_lock_and_seq;

			// Set to 1 if the bucket is write-locked. Zero otherwise. We could use a single
			// bit, but clang (and GCC before 7) will use a slow cmpxchg instead of bts to
			// test-and-set a bit. Therefore, we use a whole byte, which allows us to use
			// xchg for test-and-set.
			uint8_t write_lock;
		};
	};

	// Align up to a whole number of cachelines
	uint8_t pad[CACHELINE_BYTES][(CUCKOO_BUCKET_SIZE * sizeof(ct_entry_storage) + sizeof(uint32_t) + CACHELINE_BYTES - 1) / CACHELINE_BYTES];
} ct_bucket;

typedef union {
	struct {
		ct_common_header_storage common_header_cells[CUCKOO_BUCKET_SIZE];
		ct_type_specific_entry_storage type_specific_cells[CUCKOO_BUCKET_SIZE];
		union {
			uint32_t write_lock_and_seq;
			uint8_t write_lock;
		};
	};

	uint8_t pad[CACHELINE_BYTES][(CUCKOO_BUCKET_SIZE * (sizeof(ct_common_header_storage) + sizeof(ct_type_specific_entry_storage)) + sizeof(uint32_t) + CACHELINE_BYTES - 1) / CACHELINE_BYTES];
} ct_bucket_split;

struct cuckoo_trie {
	ct_bucket* buckets;

	// This bucket contain a single entry. This entry is a "pseudo-leaf", containing
	// no key and serving as the head of the linked list.
	// This bucket belong to the same allocation as <buckets>, so no separate free/munmap
	// is required.
	ct_bucket* min_leaf_bucket;
	uint64_t num_buckets;
	uint64_t num_pairs;
	uint64_t num_shuffle_blocks;
	uint64_t bucket_mix_table[1 << TAG_BITS];
	int is_empty;
};

struct cuckoo_trie_split {
	ct_bucket_split* buckets;

	// This bucket contain a single entry. This entry is a "pseudo-leaf", containing
	// no key and serving as the head of the linked list.
	// This bucket belong to the same allocation as <buckets>, so no separate free/munmap
	// is required.
	ct_bucket_split* min_leaf_bucket;
	uint64_t num_buckets;
	uint64_t num_pairs;
	uint64_t num_shuffle_blocks;
	uint64_t bucket_mix_table[1 << TAG_BITS];
	int is_empty;
};

static inline ct_entry_storage* trie_min_leaf(cuckoo_trie* trie) {
	return &(trie->min_leaf_bucket->cells[0]);
}

static inline ct_common_header_storage* trie_min_leaf_common_header(cuckoo_trie_split* trie) {
	return &(trie->min_leaf_bucket->common_header_cells[0]);
}

static inline ct_type_specific_entry_storage* trie_min_leaf_type_specific(cuckoo_trie_split* trie) {
	return &(trie->min_leaf_bucket->type_specific_cells[0]);
}

static inline ct_entry_descriptor trie_min_leaf_split_view(cuckoo_trie_split* trie) {
	return (ct_entry_descriptor){
		.common_header = &(trie->min_leaf_bucket->common_header_cells[0]),
		.type_specific = &(trie->min_leaf_bucket->type_specific_cells[0])
	};
}

// Functions for accessing fields in ct_entry
static inline int entry_is_secondary(ct_entry* entry) {
	return (entry->parent_color_and_flags & FLAG_SECONDARY_BUCKET) == FLAG_SECONDARY_BUCKET;
}

static inline int entry_is_secondary_common_header(ct_common_header* header) {
	return (header->parent_color_and_flags & FLAG_SECONDARY_BUCKET) == FLAG_SECONDARY_BUCKET;
}

static inline int entry_dirty(ct_entry* entry) {
#ifdef MULTITHREADING
	return (entry->parent_color_and_flags & FLAG_DIRTY) == FLAG_DIRTY;
#else
	UNUSED_PARAMETER(entry);
	return 0;
#endif
}

static inline int entry_dirty_common_header(ct_common_header* header) {
#ifdef MULTITHREADING
	return (header->parent_color_and_flags & FLAG_DIRTY) == FLAG_DIRTY;
#else
	UNUSED_PARAMETER(header);
	return 0;
#endif
}

static inline void entry_set_dirty(ct_entry* entry) {
#ifndef MULTITHREADING
	UNUSED_PARAMETER(entry);
#else
	// This function only modifies a single bit in the header but reads and writes a whole QWORD
	// to avoid store forwarding stalls when the entry is read later

	assert(offsetof(ct_entry, parent_color_and_flags) <= 7);
	uint64_t mask = FLAG_DIRTY;
	mask <<= offsetof(ct_entry, parent_color_and_flags)*8;
	*((uint64_t*)entry) |= mask;
#endif
}

static inline void entry_set_dirty_common_header(ct_common_header* header) {
#ifndef MULTITHREADING
	UNUSED_PARAMETER(header);
#else
	header->parent_color_and_flags |= FLAG_DIRTY;
#endif
}

static inline void entry_set_clean(ct_entry* entry) {
#ifndef MULTITHREADING
	UNUSED_PARAMETER(entry);
#else
	// This function only modifies a single bit in the header but reads and writes a whole QWORD
	// to avoid store forwarding stalls when the entry is read later

	assert(offsetof(ct_entry, parent_color_and_flags) <= 7);
	uint64_t mask = FLAG_DIRTY;
	mask <<= offsetof(ct_entry, parent_color_and_flags)*8;
	*((uint64_t*)entry) &= ~mask;
#endif
}

static inline void entry_set_clean_common_header(ct_common_header* header) {
#ifndef MULTITHREADING
	UNUSED_PARAMETER(header);
#else
	header->parent_color_and_flags &= ~FLAG_DIRTY;
#endif
}

static inline void entry_add_flags(ct_entry* entry, uint64_t flags) {
	assert(offsetof(ct_entry, parent_color_and_flags) <= 7);
	*((uint64_t*)entry) |= flags << (offsetof(ct_entry, parent_color_and_flags) * 8);
}

static inline void entry_add_flags_common_header(ct_common_header* header, uint64_t flags) {
	assert(offsetof(ct_common_header, parent_color_and_flags) <= 7);
	header->parent_color_and_flags |= flags;
}

static inline void entry_set_color_and_tag(ct_entry* entry, uint64_t new_value) {
	assert(offsetof(ct_entry, color_and_tag) <= 7);
	uint64_t mask = 0xffULL << (offsetof(ct_entry, color_and_tag) * 8);
	*((uint64_t*)entry) &= ~mask;
	*((uint64_t*)entry) |= new_value << (offsetof(ct_entry, color_and_tag) * 8);
}

static inline void entry_set_color_and_tag_common_header(ct_common_header* header, uint64_t new_value) {
	assert(offsetof(ct_common_header, color_and_tag) <= 7);
	new_value &= 0xff;
	header->color_and_tag = new_value;
}

static inline int entry_type(ct_entry* entry) {
	return (entry->parent_color_and_flags & TYPE_MASK);
}

static inline int entry_type_common_header(ct_common_header* header) {
	return (header->parent_color_and_flags & TYPE_MASK);
}

static inline void entry_set_child_bit(ct_entry* entry, uint64_t child) {
	assert(entry_type(entry) == TYPE_BITMAP);
	assert(sizeof(ct_entry) <= 16);

	uint64_t child_byte = offsetof(ct_entry, child_bitmap) + child/8;
	uint64_t child_pos = child_byte*8 + (7 - (child % 8));
	__uint128_t entry_val = *((__uint128_t*)entry);
	//__uint128_t mask = 1;
	//mask <<= child_pos;
	*((__uint128_t*)entry) = entry_val | (((__uint128_t)1) << child_pos);
}

static inline void entry_set_child_bit_split_view(ct_type_specific_entry* entry, ct_common_header* header, uint64_t child) {
	UNUSED_PARAMETER(header);
	#ifndef NDEBUG
	#endif
	assert(entry_type_common_header(header) == TYPE_BITMAP);
	assert(sizeof(ct_entry) <= 16);

	uint64_t child_byte = offsetof(ct_type_specific_entry, child_bitmap) + child/8;
	uint64_t child_pos = child_byte*8 + (7 - (child % 8));
	__uint128_t entry_val = *((__uint128_t*)entry);
	//__uint128_t mask = 1;
	//mask <<= child_pos;
	*((__uint128_t*)entry) = entry_val | (((__uint128_t)1) << child_pos);
}

/* Force loading the 128-bit value at <ptr> as two qwords. When using just *<ptr>,
 * unaligned loads may be used, especially if only part of the value is used, leading
 * to store-forwarding faults.
 */
static inline __uint128_t load_two_qwords(void* ptr) {
	uint64_t* qwords = (uint64_t*) ptr;
	__uint128_t result = __atomic_load_n(&(qwords[1]), __ATOMIC_RELAXED);
	result = (result << 64) | __atomic_load_n(&(qwords[0]), __ATOMIC_RELAXED);
	return result;
}

static inline __uint128_t put_bits_uint128(__uint128_t x, uint64_t offset,
										   uint64_t size, uint64_t bits) {
	__uint128_t mask = ((((__uint128_t)1) << size) - 1) << offset;
	return (x & ~mask) | (((__uint128_t)bits) << offset);
}

static inline __uint128_t get_bits_uint128(__uint128_t x, uint64_t offset,
										   uint64_t size) {
	assert(size <= 64);    //  1ULL << size will overflow otherwise
	uint64_t mask = ((1ULL << size) - 1);
	return (x >> offset) & mask;
}


// This code is unused, so commentating it out instead of adding another split_view version to it.
// static inline void entry_set_next_leaf(ct_entry* entry, ct_entry_locator* next_leaf) {
// 	assert(entry_type(entry) == TYPE_LEAF);
// 	assert(sizeof(ct_entry) <= 16);
// 	assert(sizeof(ct_entry_locator) <= 8);

// 	uint64_t next_leaf_value = 0;
// 	next_leaf_value |= ((uint64_t)(next_leaf->primary_bucket)) << (offsetof(ct_entry_locator, primary_bucket) * 8);
// 	next_leaf_value |= ((uint64_t)(next_leaf->color)) << (offsetof(ct_entry_locator, color) * 8);
// 	next_leaf_value |= ((uint64_t)(next_leaf->tag)) << (offsetof(ct_entry_locator, tag) * 8);
// 	__uint128_t entry_val = *((__uint128_t*)entry);
// 	entry_val = put_bits_uint128(entry_val, offsetof(ct_entry, next_leaf) * 8, sizeof(ct_entry_locator) * 8, next_leaf_value);
// 	*((__uint128_t*)entry) = entry_val;
// }

static inline int entry_color(ct_entry* entry) {
	return (entry->color_and_tag >> TAG_BITS);
}

static inline int entry_color_common_header(ct_common_header* header) {
	return (header->color_and_tag >> TAG_BITS);
}

static inline int entry_tag(ct_entry* entry) {
	return (entry->color_and_tag & ((1 << TAG_BITS) - 1));
}

static inline int entry_tag_common_header(ct_common_header* header) {
	return (header->color_and_tag & ((1 << TAG_BITS) - 1));
}

static inline int entry_parent_color(ct_entry* entry) {
	return (entry->parent_color_and_flags >> PARENT_COLOR_SHIFT);
}

static inline int entry_parent_color_common_header(ct_common_header* header) {
	return (header->parent_color_and_flags >> PARENT_COLOR_SHIFT);
}

static inline void entry_set_parent_color(ct_entry* entry, uint8_t color) {
	entry->parent_color_and_flags &= (1 << PARENT_COLOR_SHIFT) - 1;
	entry->parent_color_and_flags |= color << PARENT_COLOR_SHIFT;
}

static inline void entry_set_parent_color_common_header(ct_common_header* header, uint8_t color) {
	header->parent_color_and_flags &= (1 << PARENT_COLOR_SHIFT) - 1;
	header->parent_color_and_flags |= color << PARENT_COLOR_SHIFT;
}

static inline int entry_jump_size(ct_entry* entry) {
	assert(entry_type(entry) == TYPE_JUMP);
	return (entry->child_color_and_jump_size & ((1 << CHILD_COLOR_SHIFT) - 1));
}

static inline int entry_jump_size_type_specific(ct_type_specific_entry* entry) {
	// I don't want to pass the common header only for the assertion, so let's just assume there is no shenanigans.
	// assert(entry_type(entry) == TYPE_JUMP);
	return (entry->child_color_and_jump_size & ((1 << CHILD_COLOR_SHIFT) - 1));
}

static inline int entry_child_color(ct_entry* entry) {
	assert(entry_type(entry) == TYPE_JUMP);
	return (entry->child_color_and_jump_size >> CHILD_COLOR_SHIFT);
}

static inline int entry_child_color_type_specific(ct_type_specific_entry* entry) {
	// I don't want to pass the common header only for the assertion, so let's just assume there is no shenanigans.
	// assert(entry_type(entry) == TYPE_JUMP);
	return (entry->child_color_and_jump_size >> CHILD_COLOR_SHIFT);
}

static inline void entry_set_jump_size(ct_entry* entry, uint8_t jump_size) {
	assert(entry_type(entry) == TYPE_JUMP);
	entry->child_color_and_jump_size &= ~((1 << CHILD_COLOR_SHIFT) - 1);
	entry->child_color_and_jump_size |= jump_size;
}

static inline void entry_set_jump_size_type_specific(ct_type_specific_entry* entry, uint8_t jump_size) {
	// I don't want to pass the common header only for the assertion, so let's just assume there is no shenanigans.
	// assert(entry_type(entry) == TYPE_JUMP);
	entry->child_color_and_jump_size &= ~((1 << CHILD_COLOR_SHIFT) - 1);
	entry->child_color_and_jump_size |= jump_size;
}

static inline void entry_set_child_color(ct_entry* entry, uint8_t child_color) {
	assert(entry_type(entry) == TYPE_JUMP);
	entry->child_color_and_jump_size &= ((1 << CHILD_COLOR_SHIFT) - 1);
	entry->child_color_and_jump_size |= child_color << CHILD_COLOR_SHIFT;
}

static inline void entry_set_child_color_type_specific(ct_type_specific_entry* entry, uint8_t child_color) {
	// I don't want to pass the common header only for the assertion, so let's just assume there is no shenanigans.
	// assert(entry_type(entry) == TYPE_JUMP);
	entry->child_color_and_jump_size &= ((1 << CHILD_COLOR_SHIFT) - 1);
	entry->child_color_and_jump_size |= child_color << CHILD_COLOR_SHIFT;
}

static inline void entry_set_type(ct_entry* entry, uint8_t type) {
	entry->parent_color_and_flags = (entry->parent_color_and_flags & (~TYPE_MASK)) | type;
}

static inline void entry_set_type_common_header(ct_common_header* header, uint8_t type) {
	header->parent_color_and_flags = (header->parent_color_and_flags & (~TYPE_MASK)) | type;
}

static inline ct_kv* entry_kv(ct_entry* entry) {
	assert(offsetof(ct_entry, key) + sizeof(ct_small_pointer) <= 16);  // We only read 16 bytes
	__uint128_t entry_val = load_two_qwords(entry);
	uintptr_t result = get_bits_uint128(entry_val, offsetof(ct_entry, key) * 8, sizeof(ct_small_pointer) * 8);
	if (result & 0x800000000000ULL)
		result |= 0xFFFF000000000000ULL;
	return (void*)result;
}

static inline ct_kv* entry_kv_type_specific(ct_type_specific_entry* entry) {
	assert(offsetof(ct_type_specific_entry, key) + sizeof(ct_small_pointer) <= 16);  // We only read 16 bytes
	__uint128_t entry_val = load_two_qwords(entry);
	uintptr_t result = get_bits_uint128(entry_val, offsetof(ct_type_specific_entry, key) * 8, sizeof(ct_small_pointer) * 8);
	if (result & 0x800000000000ULL)
		result |= 0xFFFF000000000000ULL;
	return (void*)result;
}

static inline void entry_set_kv(ct_entry* entry, ct_kv* kv) {
	uintptr_t address_as_uint = (uintptr_t)kv;
	entry->key.low_dword = address_as_uint;
	entry->key.high_word = (address_as_uint >> 32) & 0xFFFF;
}

static inline void entry_set_kv_type_specific(ct_type_specific_entry* entry, ct_kv* kv) {
	uintptr_t address_as_uint = (uintptr_t)kv;
	entry->key.low_dword = address_as_uint;
	entry->key.high_word = (address_as_uint >> 32) & 0xFFFF;
}
