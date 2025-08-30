#include "mt_debug.h"
#include "cuckoo_trie_internal.h"
#include <string.h>

// Internal status values. Only returned from internal functions.
#define SI_OK 0
#define SI_RETRY 1
#define SI_EXISTS 2
#define SI_FAIL 3

// We might have to lock
//      - The whole path from the root to a certain leaf, inclusive (MAX_KEY_SYMBOLS + 1 locks)
//      - The predecessor of that leaf (1 lock)
// Add +1 for safety
#define MAX_LOCKS (MAX_KEY_SYMBOLS + 3)

// The least significant byte of write_lock_and_seq is the write-lock, and the sequence
// is stored in the three most significant bytes. Therefore, to increment the sequence
// number by one we add 256 to the write_lock_and_seq word
#define SEQ_INCREMENT (1 << 8)

typedef struct {
	ct_bucket* bucket;
	uint32_t entry_refcounts[CUCKOO_BUCKET_SIZE];

	// Sum of all entry refcounts, plus refcount of the bucket itself
	uint32_t total_refcount;
} ct_bucket_write_lock;

typedef struct {
	ct_bucket* bucket;
	uint32_t seq;
} ct_bucket_read_lock;

typedef struct {
	ct_bucket_split* bucket;
	uint32_t entry_refcounts[CUCKOO_BUCKET_SIZE];

	// Sum of all entry refcounts, plus refcount of the bucket itself
	uint32_t total_refcount;
} ct_bucket_write_lock_split;

typedef struct {
	ct_bucket_split* bucket;
	uint32_t seq;
} ct_bucket_read_lock_split;

typedef struct {
	ct_bucket_write_lock bucket_write_locks[MAX_LOCKS];
	ct_bucket_write_lock* next_write_lock;
	cuckoo_trie* trie;
} ct_lock_mgr;

typedef struct {
	ct_bucket_write_lock_split bucket_write_locks[MAX_LOCKS];
	ct_bucket_write_lock_split* next_write_lock;
	cuckoo_trie_split* trie;
} ct_lock_mgr_split;

static inline void init_lock_mgr(ct_lock_mgr* lock_mgr, cuckoo_trie* trie) {
	lock_mgr->trie = trie;
	lock_mgr->next_write_lock = &(lock_mgr->bucket_write_locks[0]);
}

static inline void init_lock_mgr_split(ct_lock_mgr_split* lock_mgr, cuckoo_trie_split* trie) {
	lock_mgr->trie = trie;
	lock_mgr->next_write_lock = &(lock_mgr->bucket_write_locks[0]);
}

static inline ct_bucket* bucket_containing(ct_entry_storage* entry) {
	assert(sizeof(ct_bucket) == 64);
	return (ct_bucket*)(((uintptr_t)entry) & (~(64 - 1)));
}

// Shame C doesn't have polymorphism :(
static inline ct_bucket_split* bucket_containing_common_header(ct_common_header_storage* entry) {
	assert(sizeof(ct_bucket_split) == 64);
	return (ct_bucket_split*)(((uintptr_t)entry) & (~(64 - 1)));
}

static inline ct_bucket_split* bucket_containing_type_specific_entry(ct_type_specific_entry_storage* entry) {
	assert(sizeof(ct_bucket_split) == 64);
	return (ct_bucket_split*)(((uintptr_t)entry) & (~(64 - 1)));
}

static inline uint32_t* counter_ptr(ct_entry_storage* entry) {
	return &(bucket_containing(entry)->write_lock_and_seq);
}

static inline uint32_t* counter_ptr_common_header(ct_common_header_storage* entry) {
	return &(bucket_containing_common_header(entry)->write_lock_and_seq);
}

static inline uint32_t* counter_ptr_type_specific_entry(ct_type_specific_entry_storage* entry) {
	return &(bucket_containing_type_specific_entry(entry)->write_lock_and_seq);
}

static inline uint32_t read_entry_split_view(ct_common_header_storage* common_header_entry, ct_type_specific_entry_storage* type_specific_entry, ct_common_header* common_result, ct_type_specific_entry* type_specific_result) {
	// Make sure we don't read beyond the bucket's end
	// TODO: Fix the assert, it should be modified for the split view variant.
	// assert(((uintptr_t)bucket_containing_common_header(common_header_entry)) + sizeof(ct_bucket) >= ((uintptr_t)common_header_entry) + 16);

#ifndef MULTITHREADING
	memcpy(common_result, common_header_entry, sizeof(ct_common_header));
	memcpy(type_specific_result, type_specific_entry, sizeof(ct_type_specific_entry));
	return 0;
#else
	assert(sizeof(ct_common_header) <= 4);   // We only read 1 DWORD
	assert(sizeof(ct_type_specific_entry) <= 12);   // We only read 1 QWORD and 1 DWORD.

	uint32_t seq1;
	uint32_t seq2;
	uint32_t* counter = counter_ptr_common_header(common_header_entry);
	uint32_t* common_entry = (uint32_t*)common_header_entry;
	uint32_t* common_results = (uint32_t*)common_result;
	uint64_t* type_specific_entry_parts = (uint64_t*)type_specific_entry;
	uint64_t* type_specific_results = (uint64_t*)type_specific_result;

	while (1) {
		mt_debug_wait_for_access();
		seq1 = __atomic_load_n(counter, __ATOMIC_ACQUIRE);
		mt_debug_access_done();
		if (unlikely(seq1 & SEQ_INCREMENT))
			continue;   // A write is in progress

		mt_debug_wait_for_access();
		common_results[0] = __atomic_load_n(&(common_entry[0]), __ATOMIC_ACQUIRE);
		mt_debug_access_done();

		mt_debug_wait_for_access();
		type_specific_results[0] = __atomic_load_n(&(type_specific_entry_parts[0]), __ATOMIC_ACQUIRE);
		mt_debug_access_done();

		mt_debug_wait_for_access();
		*(uint32_t*)(&type_specific_results[1]) = __atomic_load_n((uint32_t*)(&(type_specific_entry_parts[1])), __ATOMIC_ACQUIRE);
		mt_debug_access_done();

		mt_debug_wait_for_access();
		seq2 = __atomic_load_n(counter, __ATOMIC_ACQUIRE);
		mt_debug_access_done();

		if (unlikely(seq2 != seq1))
			continue;  // A write happened during the read

		break;
	}
	return seq1;
#endif
}

static inline uint32_t read_entry(ct_entry_storage* entry, ct_entry* result) {
	// Make sure we don't read beyond the bucket's end
	assert(((uintptr_t)bucket_containing(entry)) + sizeof(ct_bucket) >= ((uintptr_t)entry) + 16);

#ifndef MULTITHREADING
	*result = *((ct_entry*)entry);
	return 0;
#else
	assert(sizeof(ct_entry) <= 16);   // We only read 2 QWORDS

	uint32_t seq1;
	uint32_t seq2;
	uint32_t* counter = counter_ptr(entry);
	uint64_t* entry_parts = (uint64_t*)entry;
	uint64_t* result_parts = (uint64_t*)result;
	while (1) {
		mt_debug_wait_for_access();
		seq1 = __atomic_load_n(counter, __ATOMIC_ACQUIRE);
		mt_debug_access_done();
		if (unlikely(seq1 & SEQ_INCREMENT))
			continue;   // A write is in progress

		mt_debug_wait_for_access();
		result_parts[0] = __atomic_load_n(&(entry_parts[0]), __ATOMIC_ACQUIRE);
		mt_debug_access_done();

		mt_debug_wait_for_access();
		result_parts[1] = __atomic_load_n(&(entry_parts[1]), __ATOMIC_ACQUIRE);
		mt_debug_access_done();

		mt_debug_wait_for_access();
		seq2 = __atomic_load_n(counter, __ATOMIC_ACQUIRE);
		mt_debug_access_done();

		if (unlikely(seq2 != seq1))
			continue;  // A write happened during the read

		break;
	}
	return seq1;
#endif
}

// Read an entry. This function doesn't check the seqlock counters, so the two halves
// of the result may be inconsistent.
// It is used to ensure that the compiler only reads the entry once, and doesn't assume
// that it won't be changed by other threads.
static inline void read_entry_non_atomic(ct_entry_storage* entry, ct_entry* result) {
	// Make sure we don't read beyond the bucket's end
	assert(((uintptr_t)bucket_containing(entry)) + sizeof(ct_bucket) >= ((uintptr_t)entry) + 16);
#ifndef MULTITHREADING
	memcpy(result, entry, sizeof(ct_entry));
#else
	assert(sizeof(ct_entry) <= 16);   // We only read 2 QWORDS

	uint64_t* entry_parts = (uint64_t*)entry;
	uint64_t* result_parts = (uint64_t*)result;

	mt_debug_wait_for_access();
	result_parts[0] = __atomic_load_n(&(entry_parts[0]), __ATOMIC_ACQUIRE);
	mt_debug_access_done();

	mt_debug_wait_for_access();
	result_parts[1] = __atomic_load_n(&(entry_parts[1]), __ATOMIC_ACQUIRE);
	mt_debug_access_done();
#endif
}

// Read an entry. This function doesn't check the seqlock counters, so the two halves
// of the result may be inconsistent.
// It is used to ensure that the compiler only reads the entry once, and doesn't assume
// that it won't be changed by other threads.
static inline void read_entry_non_atomic_split_view(ct_common_header_storage* common_header_entry, ct_type_specific_entry_storage* type_specific_entry, ct_common_header* common_result, ct_type_specific_entry* type_specific_result) {
	// Make sure we don't read beyond the bucket's end
	// TODO: Fix the assertion.
	// assert(((uintptr_t)bucket_containing_common_header(common_header_entry)) + sizeof(ct_bucket) >= ((uintptr_t)common_header_entry) + 16);
#ifndef MULTITHREADING
	memcpy(common_header_result, common_header_entry, sizeof(ct_common_header));
	memcpy(type_specific_result, type_specific_entry, sizeof(ct_type_specific_entry));
#else
	assert(sizeof(common_header_entry) <= 4);   // We only read 1 DWORD
	assert(sizeof(type_specific_entry) <= 12);   // We only read 1 QWORD and 1 DWORD.

	uint32_t* common_entry = (uint32_t*)common_header_entry;
	uint32_t* common_results = (uint32_t*)common_result;
	uint64_t* type_specific_entry_parts = (uint64_t*)type_specific_entry;
	uint64_t* type_specific_results = (uint64_t*)type_specific_result;

	mt_debug_wait_for_access();
	common_results[0] = __atomic_load_n(&(common_entry[0]), __ATOMIC_ACQUIRE);
	mt_debug_access_done();

	mt_debug_wait_for_access();
	type_specific_results[0] = __atomic_load_n(&(type_specific_entry_parts[0]), __ATOMIC_ACQUIRE);
	mt_debug_access_done();

	mt_debug_wait_for_access();
	*(uint32_t*)(&type_specific_results[1]) = __atomic_load_n((uint32_t*)(&(type_specific_entry_parts[1])), __ATOMIC_ACQUIRE);
	mt_debug_access_done();
#endif
}

void write_int_atomic(uint32_t* addr, uint32_t value);
uint32_t read_int_atomic(uint32_t* addr);
uint32_t write_entry(ct_entry_storage* target, const ct_entry* src);
uint32_t write_entry_split_view(ct_common_header_storage* common_header_target, ct_type_specific_entry_storage* type_specific_target, const ct_common_header* common_src, const ct_type_specific_entry* type_specific_src);
void entry_set_parent_color_atomic(ct_entry_storage* entry, uint8_t parent_color);
uint64_t bucket_write_locked(ct_lock_mgr* lock_mgr, ct_bucket* bucket_num);
void read_lock_bucket(ct_bucket* bucket_num, ct_bucket_read_lock* read_lock);
void read_lock_bucket_split_view(ct_bucket_split* bucket, ct_bucket_read_lock_split* read_lock);
int read_unlock_bucket(ct_bucket_read_lock* read_lock);
ct_bucket_write_lock* write_lock_bucket(ct_lock_mgr* lock_mgr, ct_bucket* bucket_num);
int upgrade_bucket_lock(ct_lock_mgr* lock_mgr, ct_bucket_read_lock* read_lock);
void release_bucket_lock(ct_lock_mgr* lock_mgr, ct_bucket* bucket_num);
void release_bucket_lock_split_view(ct_lock_mgr_split* lock_mgr, ct_bucket_split* bucket_num);

void write_lock_entry_in_locked_bucket(ct_lock_mgr* lock_mgr, ct_entry_storage* entry);
void move_entry_lock(ct_lock_mgr* lock_mgr, ct_entry_storage* dst, ct_entry_storage* src);
int upgrade_lock(ct_lock_mgr* lock_mgr, ct_entry_local_copy* entry);
void upgrade_lock_wait(ct_lock_mgr* lock_mgr, ct_entry_local_copy* entry);
void write_unlock(ct_lock_mgr* lock_mgr, ct_entry_storage* entry);
void release_all_locks(ct_lock_mgr* lock_mgr);
