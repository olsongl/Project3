/**
 * dht.c
 *
 * CS 470 Project 4
 *
 * Implementation for distributed hash table (DHT).
 *
 * Name: Nick Boychenko and Gavin Olson
 *
 * Analysis: https://docs.google.com/document/d/1Qwif4KomPt-_L1IWUEgD2EOoq-Nqnu2a-vwsQcIJfpE/edit?usp=sharing
 *
 */

#include <mpi.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <string.h>

#include "dht.h"

#define TAG_RPC   0
#define TAG_SIZE  1

#define MSG_PUT      1
#define MSG_GET      2
#define MSG_TERM    -1
#define MSG_SIZE_REQ 3

static int rank;
static int nprocs;
static pthread_t server_thread;
static pthread_mutex_t local_lock = PTHREAD_MUTEX_INITIALIZER;

static volatile int size_in_progress = 0;

static int hash(const char *name) {
    unsigned h = 5381;
    while (*name != '\0') {
        h = ((h << 5) + h) + (unsigned)(*name++);
    }
    return h % nprocs;
}

static void* server_thread_func(void *arg) {
    while (1) {
        MPI_Status status;
        int flag = 0;
        MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &flag, &status);
        if (flag) {
            if (status.MPI_TAG == TAG_SIZE && rank == 0 && size_in_progress) {
                continue;
            }
            if (status.MPI_TAG == TAG_SIZE) {
                int req;
                MPI_Recv(&req, 1, MPI_INT, status.MPI_SOURCE, TAG_SIZE,
                         MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                if (req == MSG_SIZE_REQ) {
                    pthread_mutex_lock(&local_lock);
                    unsigned int local_count = local_size();
                    pthread_mutex_unlock(&local_lock);
                    MPI_Send(&local_count, 1, MPI_UNSIGNED, status.MPI_SOURCE,
                             TAG_SIZE, MPI_COMM_WORLD);
                }
                continue;
            }
            int header[2];
            MPI_Recv(header, 2, MPI_INT, status.MPI_SOURCE, status.MPI_TAG,
                     MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            int msg_type = header[0];
            if (msg_type == MSG_TERM) {
                break;
            } else if (msg_type == MSG_PUT) {
                int key_len = header[1];
                char *key = malloc(key_len);
                if (!key) {
                    fprintf(stderr, "Memory allocation error in server thread.\n");
                    MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
                }
                MPI_Recv(key, key_len, MPI_CHAR, status.MPI_SOURCE, status.MPI_TAG,
                         MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                long value;
                MPI_Recv(&value, 1, MPI_LONG, status.MPI_SOURCE, status.MPI_TAG,
                         MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                pthread_mutex_lock(&local_lock);
                local_put(key, value);
                pthread_mutex_unlock(&local_lock);

                free(key);
                int ack = 0;
                MPI_Send(&ack, 1, MPI_INT, status.MPI_SOURCE, TAG_RPC, MPI_COMM_WORLD);
            } else if (msg_type == MSG_GET) {
                int key_len = header[1];
                char *key = malloc(key_len);
                if (!key) {
                    fprintf(stderr, "Memory allocation error in server thread (get).\n");
                    MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
                }
                MPI_Recv(key, key_len, MPI_CHAR, status.MPI_SOURCE, status.MPI_TAG,
                         MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                pthread_mutex_lock(&local_lock);
                long result = local_get(key);
                pthread_mutex_unlock(&local_lock);

                free(key);
                MPI_Send(&result, 1, MPI_LONG, status.MPI_SOURCE, TAG_RPC, MPI_COMM_WORLD);
            }
        }
    }
    return NULL;
}

int dht_init()
{
    int provided;
    MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &provided);
    if (provided != MPI_THREAD_MULTIPLE) {
        fprintf(stderr, "ERROR: Cannot initialize MPI in THREAD_MULTIPLE mode.\n");
        exit(EXIT_FAILURE);
    }
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

    local_init();
    pthread_mutex_init(&local_lock, NULL); 

    int ret = pthread_create(&server_thread, NULL, server_thread_func, NULL);
    if (ret != 0) {
        fprintf(stderr, "Error creating server thread.\n");
        MPI_Abort(MPI_COMM_WORLD, ret);
    }

    return rank;
}

void dht_put(const char *key, long value)
{
    int owner = hash(key);
    if (owner == rank) {
        pthread_mutex_lock(&local_lock);
        local_put(key, value);
        pthread_mutex_unlock(&local_lock);
    } else {
        int msg_type = MSG_PUT;
        int key_len = strlen(key) + 1;
        int header[2] = {msg_type, key_len};
        MPI_Send(header, 2, MPI_INT, owner, TAG_RPC, MPI_COMM_WORLD);
        MPI_Send((void*)key, key_len, MPI_CHAR, owner, TAG_RPC, MPI_COMM_WORLD);
        MPI_Send(&value, 1, MPI_LONG, owner, TAG_RPC, MPI_COMM_WORLD);
        int ack;
        MPI_Recv(&ack, 1, MPI_INT, owner, TAG_RPC, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }
}

long dht_get(const char *key)
{
    int owner = hash(key);
    if (owner == rank) {
        pthread_mutex_lock(&local_lock);
        long result = local_get(key);
        pthread_mutex_unlock(&local_lock);
        return result;
    } else {
        int msg_type = MSG_GET;
        int key_len = strlen(key) + 1;
        int header[2] = {msg_type, key_len};
        MPI_Send(header, 2, MPI_INT, owner, TAG_RPC, MPI_COMM_WORLD);
        MPI_Send((void*)key, key_len, MPI_CHAR, owner, TAG_RPC, MPI_COMM_WORLD);
        long result;
        MPI_Recv(&result, 1, MPI_LONG, owner, TAG_RPC, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        return result;
    }
}

size_t dht_size()
{
    unsigned int local_count;
    pthread_mutex_lock(&local_lock);
    local_count = (unsigned int)local_size();
    pthread_mutex_unlock(&local_lock);
    
    if (rank == 0) {
        size_in_progress = 1;
        unsigned int global_count = local_count;
        int size_req = MSG_SIZE_REQ;
        for (int p = 1; p < nprocs; p++) {
            MPI_Send(&size_req, 1, MPI_INT, p, TAG_SIZE, MPI_COMM_WORLD);
        }
        for (int p = 1; p < nprocs; p++) {
            unsigned int remote_count;
            MPI_Recv(&remote_count, 1, MPI_UNSIGNED, p, TAG_SIZE,
                     MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            global_count += remote_count;
        }
        size_in_progress = 0;
        return global_count;
    } else {
        return local_count;
    }
}

void dht_sync()
{
    MPI_Barrier(MPI_COMM_WORLD);
}

void dht_destroy(FILE *output)
{
    int term_header[2] = {MSG_TERM, 0};
    MPI_Send(term_header, 2, MPI_INT, rank, TAG_RPC, MPI_COMM_WORLD);

    pthread_join(server_thread, NULL);
    local_destroy(output);
    pthread_mutex_destroy(&local_lock); 
    MPI_Finalize();
}
