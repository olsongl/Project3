/**
 * dht.c
 *
 * CS 470 Project 4
 *
 * Implementation for distributed hash table (DHT).
 *
 * Name: Gavin Olson and Nick Boychenko
 *
 */

 #include <mpi.h>
 #include <pthread.h>
 #include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 #include "dht.h"
 #include "local.h"
 
 #define TAG_PUT 1
 #define TAG_TERMINATE 2
 
 static int rank;
 static int nprocs;
 static pthread_t server_thread;
 static pthread_mutex_t local_lock = PTHREAD_MUTEX_INITIALIZER;

 int hash(const char *name);  // Ensure all files can use hash()

 
 typedef struct {
     char key[MAX_KEYLEN];
     long value;
 } PutRequest;
 
 void *server_loop(void *arg) {
     MPI_Status status;
     PutRequest req;
 
     while (1) {
         MPI_Recv(&req, sizeof(PutRequest), MPI_BYTE, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
 
         if (status.MPI_TAG == TAG_TERMINATE) {
             break;
         } else if (status.MPI_TAG == TAG_PUT) {
             pthread_mutex_lock(&local_lock);
             local_put(req.key, req.value);
             pthread_mutex_unlock(&local_lock);
         }
     }
 
     return NULL;
 }
 
 int dht_init() {
     int provided;
     MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &provided);
     if (provided != MPI_THREAD_MULTIPLE) {
         printf("ERROR: Cannot initialize MPI in THREAD_MULTIPLE mode.\n");
         exit(EXIT_FAILURE);
     }
 
     MPI_Comm_rank(MPI_COMM_WORLD, &rank);
     MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
 
     local_init();
 
     // Start server thread
     pthread_create(&server_thread, NULL, server_loop, NULL);
 
     return rank;
 }
 
 void dht_put(const char *key, long value) {
     int target_rank = hash(key) % nprocs;
 
     if (target_rank == rank) {
         pthread_mutex_lock(&local_lock);
         local_put(key, value);
         pthread_mutex_unlock(&local_lock);
     } else {
         PutRequest req;
         strncpy(req.key, key, MAX_KEYLEN);
         req.key[MAX_KEYLEN - 1] = '\0';
         req.value = value;
 
         MPI_Send(&req, sizeof(PutRequest), MPI_BYTE, target_rank, TAG_PUT, MPI_COMM_WORLD);
     }
 }
 
 void dht_destroy(FILE *output) {
     local_destroy(output);
 
     // Send termination signals to all server threads
     if (rank == 0) {
         for (int i = 0; i < nprocs; i++) {
             if (i != rank) {
                 MPI_Send(NULL, 0, MPI_BYTE, i, TAG_TERMINATE, MPI_COMM_WORLD);
             }
         }
     }
 
     pthread_join(server_thread, NULL);
     MPI_Finalize();
 }
 
 long dht_get(const char *key) {
    return KEY_NOT_FOUND;  // Temporary stub
}

size_t dht_size() {
    return 0;  // Temporary stub
}

void dht_sync() {
    MPI_Barrier(MPI_COMM_WORLD);  // Temporary synchronization
}


/*
 * Hash function to determine the owner process for a key.
 * Uses the djb2 algorithm: http://www.cse.yorku.ca/~oz/hash.html
 */
int hash(const char *name) {
    unsigned hash = 5381;
    while (*name != '\0') {
        hash = ((hash << 5) + hash) + (unsigned)(*name++); // hash * 33 + char
    }
    return hash % nprocs;
}
