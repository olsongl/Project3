/**
 * local.h
 *
 * CS 470 Project 4
 *
 * Private private interface for local hash table.
 *
 * DO NOT CHANGE THIS FILE!
 */

#ifndef __LOCAL_H
#define __LOCAL_H

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_KEYLEN 64

#define KEY_NOT_FOUND -1

/*
 * Local hash table function prototypes
 */
void   local_init();
void   local_put(const char *key, long value);
long   local_get(const char *key);
size_t local_size();
void   local_destroy(FILE *out);

#endif

