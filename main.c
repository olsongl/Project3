/**
 * main.c
 *
 * CS 470 Project 4
 *
 * Driver (client) code.
 *
 * DO NOT CHANGE THIS FILE!
 */

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "dht.h"

#define MAX_LINE_LEN 1024

// function prototype (strnlen is not part of POSIX in C99)
size_t strnlen(const char *str, size_t max_len);

int main(int argc, char* argv[])
{
    // each process must call sync() exactly once
    bool synced = false;

    // check command-line parameters
    if (argc != 2) {
        printf("Usage: dht <in-file>\n");
        return EXIT_FAILURE;
    }
    char *fn = argv[1];

    // initialize hash table
    int pid = dht_init();

    // open file for input
    FILE *fin = fopen(fn, "r");
    if (fin == NULL) {
        printf("ERROR: Could not open file \"%s\"\n", fn);
        return EXIT_FAILURE;
    }

    // read and process each line
    char cur_input[MAX_LINE_LEN];
    bool ignore = true;
    while (fgets(cur_input, MAX_LINE_LEN, fin) != NULL) {

        if (cur_input[0] == '#') {
            continue;       // comment; skip it
        }

        // strip trailing newline
        size_t input_len = strnlen(cur_input, MAX_LINE_LEN);
        if (cur_input[input_len-1] == '\n') {
            cur_input[input_len-1] = '\0';
        }

        // parse command
        char *cmd = strtok(cur_input, " ");
        if (cmd == NULL) {
            continue;       // blank line; skip it
        }

        //
        // handle command (essentially a giant switch)
        //
        if (strncmp(cmd, "proc", MAX_LINE_LEN) == 0) {

            // toggle handling appropriately
            char *proc = strtok(NULL, " ");
            if (proc == NULL) {
                printf("ERROR: 'proc' line missing processor ID");
                continue;
            }
            long proc_id = strtol(proc, NULL, 10);
            ignore = (proc_id != pid);

        } else if (!ignore && strncmp(cmd, "put", MAX_LINE_LEN) == 0) {

            // parse key and value
            char *key = strtok(NULL, " ");
            if (key == NULL) {
                printf("ERROR: 'put' line missing key");
                continue;
            }
            char *val = strtok(NULL, " ");
            long value = strtol(val, NULL, 10);
            if (val == NULL) {
                printf("ERROR: 'put' line missing value");
                continue;
            }

            // call DHT function
            dht_put(key, value);

        } else if (!ignore && strncmp(cmd, "get", MAX_LINE_LEN) == 0) {

            // parse key
            char *key = strtok(NULL, " ");
            if (key == NULL) {
                printf("ERROR: 'put' line missing key");
                continue;
            }

            // call DHT function and print output
            long value = dht_get(key);
            if (value == KEY_NOT_FOUND) {
                printf("Key not found: \"%s\"\n", key);
            } else {
                printf("Get(\"%s\") = %ld\n", key, value);
            }

        } else if (!ignore && strncmp(cmd, "size", MAX_LINE_LEN) == 0) {

            // call DHT function and print output
            printf("Size = %lu\n", dht_size());

        } else if (!ignore && strncmp(cmd, "sync", MAX_LINE_LEN) == 0) {

            // call DHT function
            if (!synced) {
                dht_sync();
                synced = true;
            }

        } else if (!ignore) {

            printf("Unrecognized command: \"%s\"\n", cmd);

        }
    }
    fclose(fin);

    // every process must call sync() exactly once
    if (!synced) {
        dht_sync();
    }

    // dump and clean up hash table
    char outfn[MAX_LINE_LEN];
    snprintf(outfn, MAX_LINE_LEN, "dump-%03d.txt", pid);
    FILE *fout = fopen(outfn, "w");
    if (fout == NULL) {
        printf("ERROR: Could not open file \"%s\"\n", fn);
    } else {
        fprintf(fout, "Process %d\n", pid);
        dht_destroy(fout);
        fclose(fout);
    }

    return EXIT_SUCCESS;
}

/*
 * safe string-length function (strnlen is not part of POSIX in C99)
 */
size_t strnlen(const char *str, size_t max_len)
{
    const char *end = (const char *)memchr(str, '\0', max_len);
    if (end == NULL) {
        return max_len;
    } else {
        return end - str;
    }
}

