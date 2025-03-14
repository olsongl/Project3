/**
 * local.c
 *
 * CS 470 Project 4
 *
 * Implementation for local key-value lookup table.
 *
 * DO NOT CHANGE THIS FILE!
 */

#include "local.h"

#define MAX_LOCAL_PAIRS 65536

/*
 * Private module structure: holds data for a single key-value pair
 */
struct kv_pair {
    char key[MAX_KEYLEN];
    long value;
};

/*
 * Private module variable: array that stores all local key-value pairs
 *
 * NOTE: the pairs are stored lexicographically by key for cleaner output
 */
static struct kv_pair kv_pairs[MAX_LOCAL_PAIRS];

/*
 * Private module variable: current number of actual key-value pairs
 */
static size_t pair_count;

/*
 * Helper function: search for a key in the local table. Returns the index where
 * the item should be located if it is present (useful for insertions).
 */
size_t find(const char *key)
{
    if (pair_count == 0) return 0;      // base case

    size_t lo = 0;              // lower bound (inclusive)
    size_t hi = pair_count;     // higher bound (exclusive)
    while (lo < hi-1) {
        size_t mid = (lo + hi) / 2;
        if (strncmp(key, kv_pairs[mid].key, MAX_KEYLEN) < 0) {
            hi = mid;
        } else if (strncmp(key, kv_pairs[mid].key, MAX_KEYLEN) > 0) {
            lo = mid + 1;
        } else {
            lo = mid;
        }
    }
    if (lo < pair_count && strncmp(key, kv_pairs[lo].key, MAX_KEYLEN) > 0) {
        lo++;       // adjust index for insertion if necessary
    }
    return lo;
}

/*
 * Local lookup table functions
 */

void local_init()
{
    // initialize file storage
    memset(kv_pairs, 0, sizeof(struct kv_pair) * MAX_LOCAL_PAIRS);
    pair_count = 0;
}

void local_put(const char *key, long value)
{
    int idx = find(key);
    if (idx < MAX_LOCAL_PAIRS &&
            strncmp(key, kv_pairs[idx].key, MAX_LOCAL_PAIRS) == 0) {

        // found an existing key; just change the associated value
        kv_pairs[idx].value = value;

    } else {

        // check to see if we have space for a new key-value pair
        if (pair_count >= MAX_LOCAL_PAIRS) {
            return;
        }

        // shift subsequent pairs
        for (size_t i = pair_count; i > idx; i--) {
            memcpy((void*)&kv_pairs[i], (void*)&kv_pairs[i-1], sizeof(struct kv_pair));
        }

        // insert the new key-value pair into the table
        snprintf((char*)&kv_pairs[idx].key, MAX_KEYLEN, "%s", key);
        kv_pairs[idx].value = value;
        pair_count++;
    }
}

long local_get(const char *key)
{
    size_t idx = find(key);
    if (strncmp(key, kv_pairs[idx].key, MAX_KEYLEN) == 0) {
        return kv_pairs[idx].value;
    }
    return KEY_NOT_FOUND;
}

size_t local_size()
{
    return pair_count;
}

void local_destroy(FILE *output)
{
    // print all pairs to output
    for (size_t i = 0; i < pair_count; i++) {
        fprintf(output, "  Key=\"%s\" Value=%ld\n",
                kv_pairs[i].key, kv_pairs[i].value);
    }

    // reset pair count
    pair_count = 0;
}

