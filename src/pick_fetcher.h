#ifndef PICKFETCHER_H
#define PICKFETCHER_H

#ifdef _WIN32
    #include <windows.h>
#else
    #include <pthread.h>
#endif

#include <mysql.h>
#include <time.h>
#include "config.h"

/* Configuration structure for the pick fetcher thread */
typedef struct {
    char db_host[256];
    char db_user[64];
    char db_password[64];
    char db_name[64];
    int db_port;
    char output_filepath[512];
    int update_interval_sec;    /* How often to check for new picks */
    int lookback_sec;           /* How far back to query picks */
    volatile int running;       /* Flag to signal thread shutdown */
} PickFetcherConfig;

typedef struct {
    char network[16];
    char station[16];
    char channel[16];
    char pick_time[32];
    char pick_time_ms[16];
} PickData;

typedef struct {
    PickData *picks;
    size_t count;
} PickResult;

/* Thread handle type (platform-independent) */
#ifdef _WIN32
    typedef HANDLE PickFetcherThread;
#else
    typedef pthread_t PickFetcherThread;
#endif

/* Initialize and start the pick fetcher thread */
int pickfetcher_start(PickFetcherConfig *config, PickFetcherThread *thread);

/* Stop the pick fetcher thread and wait for it to finish */
int pickfetcher_stop(PickFetcherConfig *config, PickFetcherThread thread);

/* Helper functions (can be called independently if needed) */
void format_mysql_datetime(time_t timestamp, char *buffer, size_t buffer_size);
PickResult* get_picks(time_t start_time, time_t end_time, MYSQL *conn);
void free_pick_result(PickResult *result);
int write_picks_to_file(PickResult *picks, const char *filepath,
                        time_t start_time, time_t end_time);

#endif /* PICKFETCHER_H */