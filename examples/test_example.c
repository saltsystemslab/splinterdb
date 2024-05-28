//
// Created by Aaditya Rangarajan on 3/13/24.
//
#include <unistd.h>
#include "splinterdb/default_data_config.h"
#include "splinterdb/splinterdb.h"
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <inttypes.h>
#include <pthread.h>
#include "util.h"
#include <sched.h>

#define DB_FILE_NAME    "splinterdb_intro_db"
#define DB_FILE_SIZE_MB 50000 // Size of SplinterDB device; Fixed when created
#define CACHE_SIZE_MB   256
#define USER_MAX_KEY_SIZE ((int)100)
#define SYSTEM_MAX_THREADS 32
#define MAX_LOAD_SIZE 4000000000

enum {
    YCSB,
    CUSTOM
};

typedef struct key_value_pair {
    slice key;
    slice value;
} key_value_pair;

typedef struct {
    splinterdb *spl_handle;
    uint64_t nops;
    uint64_t num_sections;
    uint64_t count_point1;
    uint64_t count_point2;
    uint64_t op[MAX_LOAD_SIZE];
    uint64_t load[MAX_LOAD_SIZE / 2];
    uint64_t run[MAX_LOAD_SIZE / 2];
    key_value_pair * kvp;
} ThreadArgs;


void timer_start(uint64_t *timer) {
    struct timeval start_time;
    assert(!gettimeofday(&start_time, NULL));
    *timer = 1000000 * start_time.tv_sec + start_time.tv_usec;
}

void timer_stop(uint64_t *timer) {
    struct timeval stop_time;
    assert(!gettimeofday(&stop_time, NULL));
    *timer = (1000000 * stop_time.tv_sec + stop_time.tv_usec) - *timer;
}


int next_command(FILE *input, int *op, uint64_t *arg, int mode) {
    int ret;
    char command[64];
    char *insert = mode == YCSB ? "I" : "Inserting";
    char *read = mode == YCSB ? "R" : "Query";
    char *update = mode == YCSB ? "U" : "Updating";
    ret = fscanf(input, "%s %ld", command, arg);
    if (ret == EOF)
        return EOF;
    else if (ret != 2) {
        fprintf(stderr, "Parse error\n");
        exit(3);
    }

    if (strcmp(command, insert) == 0) {
        *op = 0;
    } else if (strcmp(command, update) == 0) {
        *op = 1;
    } else if (strcmp(command, "Deleting") == 0) {
        *op = 2;
    } else if (strcmp(command, read) == 0) {
        *op = 3;
        if (mode == CUSTOM) {
            if (1 != fscanf(input, " -> %s", command)) {
                fprintf(stderr, "Parse error\n");
                exit(3);
            }
        }
    } else if (strcmp(command, "Full_scan") == 0) {
        *op = 4;
    } else if (strcmp(command, "Lower_bound_scan") == 0) {
        *op = 5;
    } else if (strcmp(command, "Upper_bound_scan") == 0) {
        *op = 6;
    } else {
        fprintf(stderr, "Unknown command: %s\n", command);
        exit(1);
    }

    return 0;
}

void* run_upserts(void * arg) {
    ThreadArgs* thread_args = (ThreadArgs*)arg;

    // Access the arguments
    splinterdb* spl_handle = thread_args->spl_handle;
    uint64_t nops = thread_args->nops;
    uint64_t num_sections = thread_args->num_sections;
    uint64_t count_point1 = thread_args->count_point1;
    uint64_t count_point2 = thread_args->count_point2;
    uint64_t ops[nops];
    uint64_t load[count_point1];
    memcpy(ops, thread_args->op, sizeof(uint64_t) * MAX_LOAD_SIZE);
    memcpy(load, thread_args->load, sizeof(uint64_t) * MAX_LOAD_SIZE / 2);
    //! Each thread will run this function. They will pass their portion of the
    //! input. We will stop the timer when number of operations is nops1/2/3.
    int thread_id = sched_getcpu();
    int start_index = (count_point1 / 2) * (uint64_t) (thread_id % SYSTEM_MAX_THREADS);
    int end_index = start_index + (count_point1 / SYSTEM_MAX_THREADS);
    slice key, value;
    //int w = 0;
    splinterdb_lookup_result  result;
    for (int i = start_index; i < end_index; i++) {
        if (ops[i] == 0) {
            key = slice_create((size_t) strlen((const char *) load[i]), (const void *) load[i]);
            value = slice_create((size_t) strlen((const char *) load[i]), (const void *) load[i]);
            splinterdb_insert(spl_handle, key, value);
//            struct key_value_pair kv = {key, value};
//            kvp[w++] = kv;
            break;
        } else if (ops[i] == 1) {
            key = slice_create((size_t) strlen((const char *) load[i]), (const void *) load[i]);
            value = slice_create((size_t) strlen((const char *) load[i]), (const void *) load[i]);
            splinterdb_insert(spl_handle, key, value);
            break;
        }
    }
}

void* run_queries(void * arg) {
    ThreadArgs* thread_args = (ThreadArgs*)arg;

    // Access the arguments
    splinterdb* spl_handle = thread_args->spl_handle;
    uint64_t nops = thread_args->nops;
    uint64_t num_sections = thread_args->num_sections;
    uint64_t count_point1 = thread_args->count_point1;
    uint64_t count_point2 = thread_args->count_point2;
    uint64_t ops[nops];
    uint64_t run[count_point2];
    memcpy(ops, thread_args->op, sizeof(uint64_t) * MAX_LOAD_SIZE);
    memcpy(run, thread_args->run, sizeof(uint64_t) * MAX_LOAD_SIZE / 2);
    //! Each thread will run this function. They will pass their portion of the
    //! input. We will stop the timer when number of operations is nops1/2/3.
    int thread_id = sched_getcpu();
    int start_index = (count_point2 / 2) * (uint64_t) (thread_id % SYSTEM_MAX_THREADS);
    int end_index = start_index + (count_point2 / SYSTEM_MAX_THREADS);
    slice key, value;
    //int w = 0;
    splinterdb_lookup_result  result;
    for (int i = start_index; i < end_index; i++) {
        if (ops[i] == 3) {
            splinterdb_lookup_result_init(spl_handle, &result, 0, NULL);
            key = slice_create((size_t) strlen((const char *) run[i]), (const void *) run[i]);
            slice lookup;
            printf("\nLookup %lu\n", i);
            splinterdb_lookup(spl_handle, key, &result);
            splinterdb_lookup_result_value(&result, &lookup);
        }
    }
}


int test(splinterdb *spl_handle, FILE *script_input, uint64_t nops,
         uint64_t num_sections,
         uint64_t count_point1,
         uint64_t count_point2,
         uint64_t count_point3,
         uint64_t count_point4,
         uint64_t count_point5,
         uint64_t count_point6, int mode) {
    key_value_pair *kvp = (key_value_pair *) malloc(nops/2 * sizeof(key_value_pair));
    slice key, value;;

    splinterdb_lookup_result result;

    uint64_t timer = 0;
    uint64_t count_points_array[] = {count_point1, count_point2,
                                     count_point3, count_point4,
                                     count_point5, count_point6};
    double timer_array[100];
    uint64_t num_of_loads_array[100];
    uint64_t num_of_stores_array[100];
    uint64_t section_index = 0;
    uint64_t load[count_point1];
    uint64_t run[count_point2];
    uint64_t opcodes[nops];
    int a = 0, b = 0, c = 0;
    int query_start_index = -1;
    memset(&opcodes[0], 0x00, nops * sizeof(uint64_t));
    memset(&load[0], 0x00, (count_point1) * sizeof(uint64_t));
    memset(&run[0], 0x00, (count_point2) * sizeof(uint64_t));
    //uint64_t w = 0;
    timer_start(&timer);
    splinterdb_flush_count(spl_handle);
    for (uint64_t i = 1; i <= nops; i++) {
        int op;
        uint64_t u;
        char t[100];
        if (script_input) {
            int r = next_command(script_input, &op, &u, mode);
            if (r == EOF)
                exit(0);
            else if (r < 0)
                exit(4);
        } else {
            op = rand() % 7;
            u = rand() % 100000;
        }
        opcodes[a++] = op;
        if (op == 0 || op == 1) {
            load[b++] = u;
        } else {
            if (query_start_index == -1) {
                query_start_index = i;
            }
            run[c++] = u;
        }
    }

    {
        pthread_t threads[32];
        timer_start(&timer);
        for (int i = 0; i < 32; i++) {
            ThreadArgs *thread_args = (ThreadArgs *) malloc(sizeof(ThreadArgs));
            thread_args->spl_handle = spl_handle;
            thread_args->nops = nops;
            thread_args->num_sections = num_sections;
            thread_args->count_point1 = count_point1;
            thread_args->count_point2 = count_point2;
            memcpy(thread_args->op, opcodes, sizeof(uint64_t) * MAX_LOAD_SIZE);
            memcpy(thread_args->run, opcodes, sizeof(uint64_t) * MAX_LOAD_SIZE / 2);
            memcpy(thread_args->load, opcodes, sizeof(uint64_t) * MAX_LOAD_SIZE / 2);
            int c_result = pthread_create(&threads[i], NULL, run_upserts, (void *) thread_args);
            if (c_result != 0) {
                fprintf(stderr, "Error creating thread %d\n", i);
                return 1;
            }
        }
        for (int i = 0; i < 32; i++) {
            int c_result = pthread_join(threads[i], NULL);
            if (c_result != 0) {
                fprintf(stderr, "Error joining thread %d\n", i);
                return 1;
            }
        }
        timer_stop(&timer);
        printf("Timer for first phase %lu", timer);
    }

    {
        pthread_t threads[32];
        timer_start(&timer);
        for (int i = 0; i < 32; i++) {
            ThreadArgs *thread_args = (ThreadArgs *) malloc(sizeof(ThreadArgs));
            thread_args->spl_handle = spl_handle;
            thread_args->nops = nops;
            thread_args->num_sections = num_sections;
            thread_args->count_point1 = count_point1;
            thread_args->count_point2 = count_point2;
            memcpy(thread_args->op, opcodes, sizeof(uint64_t) * MAX_LOAD_SIZE);
            memcpy(thread_args->run, opcodes, sizeof(uint64_t) * MAX_LOAD_SIZE / 2);
            memcpy(thread_args->load, opcodes, sizeof(uint64_t) * MAX_LOAD_SIZE / 2);
            int c_result = pthread_create(&threads[i], NULL, run_queries, (void *) thread_args);
            if (c_result != 0) {
                fprintf(stderr, "Error creating thread %d\n", i);
                return 1;
            }
        }
        for (int i = 0; i < 32; i++) {
            int c_result = pthread_join(threads[i], NULL);
            if (c_result != 0) {
                fprintf(stderr, "Error joining thread %d\n", i);
                return 1;
            }
        }
        timer_stop(&timer);
        printf("Timer for first phase %lu", timer);
    }

    printf("Test PASSED\n");
    printf("######## Test result of splinterDB ########");
    splinterdb_flush_count(spl_handle);
    double total_runtime = 0;
    uint64_t total_num_of_loads = 0;
    uint64_t total_num_of_stores = 0;

    // print the runtime for each phase
    for (uint64_t i = 0; i < num_sections; i++) {
        total_runtime += timer_array[i];
        printf("\nPhase %" PRIu64 " runtime: %f. Timer stop at the %" PRIu64 "th operation.\n",
               i + 1, timer_array[i], count_points_array[i]);

        uint64_t curr_phase_num_of_loads = num_of_loads_array[i];
        uint64_t curr_phase_num_of_stores = num_of_stores_array[i];
        total_num_of_loads += curr_phase_num_of_loads;
        total_num_of_stores += curr_phase_num_of_stores;

        printf("Number of loads: %" PRIu64 "\n", curr_phase_num_of_loads);
        printf("Number of stores: %" PRIu64 "\n", curr_phase_num_of_stores);
        printf("Total IO: %" PRIu64 "\n", curr_phase_num_of_loads + curr_phase_num_of_stores);
    }

    printf("\nTotal number of loads: %" PRIu64"\n", total_num_of_loads);
    printf("Total number of stores: %" PRIu64 "\n", total_num_of_stores);
    printf("Total IO: %" PRIu64 "\n", total_num_of_loads + total_num_of_stores);


    return 0;
}


int main(int argc, char **argv) {
    char *script_infile = NULL;
    unsigned int random_seed = time(NULL) * getpid();
    srand(random_seed);
    int opt;
    char *term;
    int mode;
    int nops = 0;
    uint64_t num_sections = 2;
    uint64_t count_point1 = UINT64_MAX;
    uint64_t count_point2 = UINT64_MAX;
    uint64_t count_point3 = UINT64_MAX;
    uint64_t count_point4 = UINT64_MAX;
    uint64_t count_point5 = UINT64_MAX;
    uint64_t count_point6 = UINT64_MAX;

    while ((opt = getopt(argc, argv, "m:i:n:t:u:v:w:x:y:z:")) != -1) {
        switch (opt) {
            case 'm':
                if (strtoull(optarg, &term, 10) == 0) {
                    mode = YCSB;
                } else {
                    mode = CUSTOM;
                }
                break;
            case 'i':
                script_infile = optarg;
                break;
            case 'n':
                nops = strtoull(optarg, &term, 10);
                break;
            case 't':
                num_sections = strtoull(optarg, &term, 10);
                break;
            case 'u':
                count_point1 = strtoull(optarg, &term, 10);
                break;
            case 'v':
                count_point2 = strtoull(optarg, &term, 10);
                break;
            case 'w':
                count_point3 = strtoull(optarg, &term, 10);
                break;
            case 'x':
                count_point4 = strtoull(optarg, &term, 10);
                break;
            case 'y':
                count_point5 = strtoull(optarg, &term, 10);
                break;
            case 'z':
                count_point6 = strtoull(optarg, &term, 10);
                if (count_point6 != UINT64_MAX) {
                    nops = count_point6;
                }
                break;


            default:
                exit(1);
        }
    }

    FILE *script_input = NULL;
    if (script_infile) {
        script_input = fopen(script_infile, "r");
        if (script_input == NULL) {
            perror("Couldn't open input file");
            exit(1);
        }
    }
    data_config splinter_data_cfg;
    default_data_config_init(USER_MAX_KEY_SIZE, &splinter_data_cfg);

    // Basic configuration of a SplinterDB instance
    splinterdb_config splinterdb_cfg;
    memset(&splinterdb_cfg, 0, sizeof(splinterdb_cfg));
    splinterdb_cfg.filename = DB_FILE_NAME;
    splinterdb_cfg.disk_size = ((uint64) DB_FILE_SIZE_MB * 1024 * 1024);
    splinterdb_cfg.cache_size = ((uint64) CACHE_SIZE_MB * 1024 * 1024);
    splinterdb_cfg.data_cfg = &splinter_data_cfg;

    splinterdb *spl_handle = NULL; // To a running SplinterDB instance

    int rc = splinterdb_create(&splinterdb_cfg, &spl_handle);
    printf("Created SplinterDB instance, dbname '%s'.\n\n", DB_FILE_NAME);
    uint64_t timer = 0;
    timer_start(&timer);
    test(spl_handle, script_input, nops, num_sections,
         count_point1, count_point2, count_point3, count_point4, count_point5, count_point6, mode);
    timer_stop(&timer);
    splinterdb_print_stats(spl_handle);
    splinterdb_close(&spl_handle);
    return rc;
}
