#ifndef RINGCLIENT_H
#define RINGCLIENT_H

#ifdef _WIN32
    #include <winsock2.h>
    #include <windows.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <ctype.h>
#include <errno.h>
#include <sys/stat.h>

#include <libslink.h>
#include "config.h"

/* Ring buffer configuration - can be overridden at runtime */
#define DEFAULT_RING_BUFFER_MINUTES 5
#define DEFAULT_CLEANUP_INTERVAL 100
#define MSEED_RECORD_SIZE 512
#define MAX_FILENAME 256

/* Structure to track ring buffer state for each stream */
typedef struct RingBuffer {
    char filename[MAX_FILENAME];
    char streamid[64];
    char selector[16];
    double oldest_time;
    double newest_time;
    long record_count;
    struct RingBuffer *next;
} RingBuffer;

typedef struct {
    char streamid[64];
    char selector[16];
} StreamSubscription;

/* Runtime configuration for ringclient */
typedef struct {
    char server_address[256];
    int port;
    char stream_file[512];
    char state_file[512];
    char output_dir[512];
    int verbose;
    int ring_buffer_minutes;
    int cleanup_interval;      /* Clean old records every N packets */
    volatile int running;      /* Flag to signal shutdown */
} RingClientConfig;

/* Thread handle type */
#ifdef _WIN32
    typedef HANDLE RingClientThread;
#else
    #include <pthread.h>
    typedef pthread_t RingClientThread;
#endif

/* Public API for ringclient */

/* Initialize configuration with defaults */
void ringclient_init_config(RingClientConfig *config);

/* Start ringclient in a separate thread */
int ringclient_start(RingClientConfig *config, RingClientThread *thread);

/* Stop ringclient thread */
int ringclient_stop(RingClientConfig *config, RingClientThread thread);

/* Run ringclient in the current thread (blocking) */
int ringclient_run(RingClientConfig *config);

/* Cleanup resources */
void ringclient_cleanup(void);

#endif /* RINGCLIENT_H */