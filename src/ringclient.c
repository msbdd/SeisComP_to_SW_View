/*
 * RingClient - SeedLink client with ring buffer capability
 * Derived from the example of the seedlink client from libslink
 */
#include "ringclient.h"

/* Module-level state */
static int g_verbose = 0;
static int g_ring_buffer_minutes = DEFAULT_RING_BUFFER_MINUTES;
static int g_cleanup_interval = DEFAULT_CLEANUP_INTERVAL;
static char g_output_dir[512] = ".";

static StreamSubscription *subscriptions = NULL;
static int subscription_count = 0;
static RingBuffer *ring_buffers = NULL;

/* Pointer to the config so we can check running flag */
static volatile int *g_running_ptr = NULL;

/* Forward declarations for internal functions */
static void packet_handler(SLCD *slconn, const SLpacketinfo *packetinfo,
                           const char *payload, uint32_t payloadlength);
static void sanitize_selector_for_filename(const char *selector, char *sanitized, size_t len);
static void create_filename_from_streamid(const char *streamid, const char *selector, 
                                          char *filename, size_t len);
static void add_subscription(const char *streamid, const char *selector);
static void cleanup_subscriptions(void);
static const char* find_matching_selector(const char *streamid, const char *loc_channel);
static void extract_selector_from_miniseed(const char *mseed_record, char *loc_channel, size_t len);
static double extract_miniseed_time(const char *mseed_record);
static RingBuffer* get_or_create_ringbuffer(const char *streamid, const char *selector);
static int write_packet_to_ringbuffer(RingBuffer *rb, const char *payload,
                                      uint32_t payloadlen, double datatime);
static int cleanup_old_records(RingBuffer *rb, double current_time);
static void ringbuffer_cleanup(void);
static int load_stream_file(SLCD *slconn, const char *streamfile);

/* ============================================================================
 * PUBLIC API IMPLEMENTATION
 * ============================================================================ */

void ringclient_init_config(RingClientConfig *config) {
    memset(config, 0, sizeof(RingClientConfig));
    strcpy(config->server_address, "localhost");
    config->port = 18000;
    config->stream_file[0] = '\0';
    config->state_file[0] = '\0';
    strcpy(config->output_dir, ".");
    config->verbose = 0;
    config->ring_buffer_minutes = DEFAULT_RING_BUFFER_MINUTES;
    config->cleanup_interval = DEFAULT_CLEANUP_INTERVAL;
    config->running = 0;
}

/* Internal run function - does the actual work */
static int ringclient_run_internal(RingClientConfig *config) {
    SLCD *slconn = NULL;
    const SLpacketinfo *packetinfo = NULL;
    char *plbuffer = NULL;
    uint32_t plbuffersize = 16384;
    char server_str[300];
    int status;

    /* Set module-level configuration */
    g_verbose = config->verbose;
    g_ring_buffer_minutes = config->ring_buffer_minutes;
    g_cleanup_interval = config->cleanup_interval;
    g_running_ptr = &config->running;
    strncpy(g_output_dir, config->output_dir, sizeof(g_output_dir) - 1);

    /* Initialize SeedLink connection */
    slconn = sl_initslcd(PACKAGE, VERSION);
    if (slconn == NULL) {
        fprintf(stderr, "[RingClient] Failed to initialize SeedLink connection\n");
        return -1;
    }

    /* Build server address string */
    if (config->port != 18000) {
        snprintf(server_str, sizeof(server_str), "%s:%d", 
                 config->server_address, config->port);
    } else {
        strncpy(server_str, config->server_address, sizeof(server_str) - 1);
    }

    sl_set_serveraddress(slconn, server_str);
    
    /* 
     * Set libslink verbosity:
     * Our verbose=0 -> libslink 0 (quiet)
     * Our verbose=1 -> libslink 0 (we handle our own messages)
     * Our verbose=2 -> libslink 1 (show libslink diagnostic messages)
     * Our verbose=3 -> libslink 2 (show libslink debug messages)
     */
    int sl_verbosity = (g_verbose >= 2) ? (g_verbose - 1) : 0;
    sl_loginit(sl_verbosity, NULL, NULL, NULL, NULL);
    
    printf("[RingClient] Connecting to %s\n", server_str);
    printf("[RingClient] Verbose level: %d\n", g_verbose);
    if (g_verbose >= 2) {
        printf("[RingClient] Debug mode: will show per-packet info\n");
    }
    printf("[RingClient] Cleanup interval: every %d packets\n", g_cleanup_interval);

    /* Load stream file if specified */
    if (config->stream_file[0] != '\0') {
        if (load_stream_file(slconn, config->stream_file) < 0) {
            fprintf(stderr, "[RingClient] Failed to load stream file: %s\n", 
                    config->stream_file);
            sl_freeslcd(slconn);
            return -1;
        }
    } else {
        /* Subscribe to all stations with default selectors */
        sl_set_allstation_params(slconn, NULL, SL_UNSETSEQUENCE, NULL);
    }

    /* Restore state if state file specified */
    if (config->state_file[0] != '\0') {
        if (sl_recoverstate(slconn, config->state_file) < 0) {
            if (g_verbose >= 1)
                printf("[RingClient] No previous state to recover\n");
        }
    }

    /* Allocate payload buffer */
    plbuffer = (char *)malloc(plbuffersize);
    if (plbuffer == NULL) {
        fprintf(stderr, "[RingClient] Memory allocation failed\n");
        sl_freeslcd(slconn);
        return -1;
    }

    printf("[RingClient] Starting main loop (ring buffer: %d minutes)\n", 
           g_ring_buffer_minutes);

    /* Main collection loop - check config->running flag */
    while (config->running) {
        status = sl_collect(slconn, &packetinfo, plbuffer, plbuffersize);
        
        if (status == SLPACKET) {
            packet_handler(slconn, packetinfo, plbuffer, packetinfo->payloadcollected);
        }
        else if (status == SLTERMINATE) {
            printf("[RingClient] Received terminate signal from libslink\n");
            break;
        }
        else if (status == SLTOOLARGE) {
            fprintf(stderr, "[RingClient] Payload too large: %u > %u\n",
                    packetinfo->payloadlength, plbuffersize);
            break;
        }
        else if (status == SLAUTHFAIL) {
            fprintf(stderr, "[RingClient] Authentication failed\n");
            break;
        }
        else if (status == SLNOPACKET) {
            sl_usleep(100000);  /* 100ms */
        }
    }

    /* Cleanup */
    printf("[RingClient] Shutting down...\n");
    
    sl_disconnect(slconn);

    if (config->state_file[0] != '\0') {
        sl_savestate(slconn, config->state_file);
    }

    ringbuffer_cleanup();
    cleanup_subscriptions();
    sl_freeslcd(slconn);
    free(plbuffer);

    printf("[RingClient] Stopped\n");
    return 0;
}

int ringclient_run(RingClientConfig *config) {
    config->running = 1;
    return ringclient_run_internal(config);
}

/* Thread entry point */
#ifdef _WIN32
static DWORD WINAPI ringclient_thread_func(LPVOID arg)
#else
static void* ringclient_thread_func(void *arg)
#endif
{
    RingClientConfig *config = (RingClientConfig*)arg;
    ringclient_run_internal(config);
    
#ifdef _WIN32
    return 0;
#else
    return NULL;
#endif
}

int ringclient_start(RingClientConfig *config, RingClientThread *thread) {
    config->running = 1;

#ifdef _WIN32
    *thread = CreateThread(NULL, 0, ringclient_thread_func, config, 0, NULL);
    if (*thread == NULL) {
        fprintf(stderr, "[RingClient] Failed to create thread\n");
        return -1;
    }
#else
    if (pthread_create(thread, NULL, ringclient_thread_func, config) != 0) {
        fprintf(stderr, "[RingClient] Failed to create thread\n");
        return -1;
    }
#endif

    return 0;
}

int ringclient_stop(RingClientConfig *config, RingClientThread thread) {
    printf("[RingClient] Stop requested\n");
    config->running = 0;

#ifdef _WIN32
    WaitForSingleObject(thread, 10000);
    CloseHandle(thread);
#else
    pthread_join(thread, NULL);
#endif

    return 0;
}

void ringclient_cleanup(void) {
    ringbuffer_cleanup();
    cleanup_subscriptions();
}

/* ============================================================================
 * INTERNAL FUNCTIONS
 * ============================================================================ */

static void
sanitize_selector_for_filename(const char *selector, char *sanitized, size_t len)
{
    size_t i;
    
    if (len == 0)
        return;
    
    strncpy(sanitized, selector, len - 1);
    sanitized[len - 1] = '\0';
    
    for (i = 0; i < strlen(sanitized); i++)
    {
        if (sanitized[i] == '?' || sanitized[i] == '*' || sanitized[i] == ' ' ||
            sanitized[i] == ':' || sanitized[i] == '"' || sanitized[i] == '/' ||
            sanitized[i] == '\\' || sanitized[i] == '|' || sanitized[i] == '<' ||
            sanitized[i] == '>' || !isprint((unsigned char)sanitized[i]))
        {
            sanitized[i] = '_';
        }
    }
}

static void 
create_filename_from_streamid(const char *streamid, const char *selector, 
                              char *filename, size_t len)
{
    char sanitized_selector[16] = {0};
    
    if (selector && selector[0] != '\0')
    {
        sanitize_selector_for_filename(selector, sanitized_selector, sizeof(sanitized_selector));
        snprintf(filename, len, "%s/%s_%s.mseed", g_output_dir, streamid, sanitized_selector);
    }
    else
    {
        snprintf(filename, len, "%s/%s.mseed", g_output_dir, streamid);
    }
}

static void
add_subscription(const char *streamid, const char *selector)
{
    StreamSubscription *new_subs;
    
    new_subs = (StreamSubscription *)realloc(subscriptions, 
                                             (subscription_count + 1) * sizeof(StreamSubscription));
    if (new_subs == NULL)
    {
        fprintf(stderr, "[RingClient] Failed to allocate subscription\n");
        return;
    }
    
    subscriptions = new_subs;
    strncpy(subscriptions[subscription_count].streamid, streamid, 
            sizeof(subscriptions[subscription_count].streamid) - 1);
    subscriptions[subscription_count].streamid[sizeof(subscriptions[subscription_count].streamid) - 1] = '\0';
    strncpy(subscriptions[subscription_count].selector, selector, 
            sizeof(subscriptions[subscription_count].selector) - 1);
    subscriptions[subscription_count].selector[sizeof(subscriptions[subscription_count].selector) - 1] = '\0';
    
    /* Show subscription info at verbose >= 1 */
    if (g_verbose >= 1)
        printf("[RingClient] Subscription: %s:%s\n", streamid, selector);
    
    subscription_count++;
}

static void
cleanup_subscriptions(void)
{
    if (subscriptions != NULL)
    {
        free(subscriptions);
        subscriptions = NULL;
    }
    subscription_count = 0;
}

static const char*
find_matching_selector(const char *streamid, const char *loc_channel)
{
    int i, j;
    int matches;
    int wildcard_count;
    int best_match_idx = -1;
    int best_wildcard_count = 999;
    
    for (i = 0; i < subscription_count; i++)
    {
        if (strcmp(subscriptions[i].streamid, streamid) != 0)
            continue;
        
        const char *selector = subscriptions[i].selector;
        int selector_len = (int)strlen(selector);
        matches = 1;
        wildcard_count = 0;
        
        if (selector_len == 5)
        {
            for (j = 0; j < 5; j++)
            {
                if (selector[j] == '?')
                    wildcard_count++;
                else if (selector[j] != loc_channel[j])
                {
                    matches = 0;
                    break;
                }
            }
        }
        else if (selector_len == 3)
        {
            for (j = 0; j < 3; j++)
            {
                if (selector[j] == '?')
                    wildcard_count++;
                else if (selector[j] != loc_channel[j + 2])
                {
                    matches = 0;
                    break;
                }
            }
        }
        else
        {
            matches = 0;
        }
        
        if (matches && wildcard_count < best_wildcard_count)
        {
            best_wildcard_count = wildcard_count;
            best_match_idx = i;
        }
    }
    
    if (best_match_idx >= 0)
        return subscriptions[best_match_idx].selector;
    
    return loc_channel;
}

static void
extract_selector_from_miniseed(const char *mseed_record, char *loc_channel, size_t len)
{
    if (len > 5)
    {
        char loc[3] = {0};
        char chan[4] = {0};
        int i;
        
        strncpy(loc, &mseed_record[13], 2);
        loc[2] = '\0';
        strncpy(chan, &mseed_record[15], 3);
        chan[3] = '\0';
        
        snprintf(loc_channel, len, "%s%s", loc, chan);
        
        for (i = 0; i < 5; i++)
        {
            if (loc_channel[i] == '?' || loc_channel[i] == '*' || 
                loc_channel[i] == ' ' || !isprint((unsigned char)loc_channel[i]))
            {
                loc_channel[i] = '_';
            }
        }
    }
    else if (len > 0)
    {
        loc_channel[0] = '\0';
    }
}

static double 
extract_miniseed_time(const char *mseed_record)
{
    struct tm timeinfo = {0};
    unsigned short year, day, fraction;
    unsigned char hour, minute, second;
    time_t epoch;
    
    year = (unsigned char)mseed_record[20] << 8 | (unsigned char)mseed_record[21];
    day = (unsigned char)mseed_record[22] << 8 | (unsigned char)mseed_record[23];
    hour = (unsigned char)mseed_record[24];
    minute = (unsigned char)mseed_record[25];
    second = (unsigned char)mseed_record[26];
    fraction = (unsigned char)mseed_record[27] << 8 | (unsigned char)mseed_record[28];
    
    timeinfo.tm_year = year - 1900;
    timeinfo.tm_mon = 0;
    timeinfo.tm_mday = day;
    timeinfo.tm_hour = hour;
    timeinfo.tm_min = minute;
    timeinfo.tm_sec = second;
    timeinfo.tm_isdst = -1;
    
    epoch = mktime(&timeinfo);
    
    return (double)epoch + (double)fraction * 0.0001;
}

static RingBuffer* 
get_or_create_ringbuffer(const char *streamid, const char *selector)
{
    RingBuffer *rb = ring_buffers;
    
    while (rb != NULL)
    {
        if (strcmp(rb->streamid, streamid) == 0 && strcmp(rb->selector, selector) == 0)
            return rb;
        rb = rb->next;
    }
    
    rb = (RingBuffer *)calloc(1, sizeof(RingBuffer));
    if (rb == NULL)
    {
        fprintf(stderr, "[RingClient] Failed to allocate ring buffer\n");
        return NULL;
    }
    
    strncpy(rb->streamid, streamid, sizeof(rb->streamid) - 1);
    strncpy(rb->selector, selector, sizeof(rb->selector) - 1);
    create_filename_from_streamid(streamid, selector, rb->filename, sizeof(rb->filename));
    
    rb->next = ring_buffers;
    ring_buffers = rb;
    
    /* Always show new buffer creation */
    printf("[RingClient] Created buffer: %s -> %s\n", streamid, rb->filename);
    
    return rb;
}

static int
cleanup_old_records(RingBuffer *rb, double current_time)
{
    FILE *fp = NULL;
    FILE *tmp_fp = NULL;
    char tmp_filename[MAX_FILENAME + 8];
    char record_buffer[MSEED_RECORD_SIZE];
    double cutoff_time = current_time - (g_ring_buffer_minutes * 60.0);
    long records_kept = 0;
    long records_removed = 0;

    fp = fopen(rb->filename, "rb");
    if (fp == NULL)
        return 0;

    snprintf(tmp_filename, sizeof(tmp_filename), "%s.tmp", rb->filename);

    tmp_fp = fopen(tmp_filename, "wb");
    if (tmp_fp == NULL)
    {
        fclose(fp);
        return -1;
    }

    while (fread(record_buffer, 1, MSEED_RECORD_SIZE, fp) == MSEED_RECORD_SIZE)
    {
        double record_time = extract_miniseed_time(record_buffer);

        if (record_time >= cutoff_time)
        {
            if (fwrite(record_buffer, 1, MSEED_RECORD_SIZE, tmp_fp) != MSEED_RECORD_SIZE)
            {
                fclose(fp);
                fclose(tmp_fp);
                remove(tmp_filename);
                return -1;
            }
            records_kept++;

            if (records_kept == 1)
                rb->oldest_time = record_time;
        }
        else
        {
            records_removed++;
        }
    }

    fclose(fp);
    fclose(tmp_fp);

#ifdef _WIN32
    if (!ReplaceFileA(rb->filename, tmp_filename, NULL, 
                      REPLACEFILE_IGNORE_MERGE_ERRORS, NULL, NULL))
    {
        DWORD error = GetLastError();
        if (error == ERROR_FILE_NOT_FOUND)
        {
            if (rename(tmp_filename, rb->filename) != 0)
            {
                remove(tmp_filename);
                return -1;
            }
        }
        else
        {
            remove(tmp_filename);
            return -1;
        }
    }
#else
    if (rename(tmp_filename, rb->filename) != 0)
    {
        remove(tmp_filename);
        return -1;
    }
#endif

    rb->record_count = records_kept;

    /* Show cleanup info at verbose >= 1, but only if records were removed */
    if (records_removed > 0 && g_verbose >= 1)
    {
        printf("[RingClient] Cleaned %ld old records from %s (kept %ld)\n", 
               records_removed, rb->filename, records_kept);
    }

    return (int)records_removed;
}

static int 
write_packet_to_ringbuffer(RingBuffer *rb, const char *payload, 
                           uint32_t payloadlen, double datatime)
{
    FILE *fp = NULL;
    
    /* Use configurable cleanup interval */
    if (g_cleanup_interval > 0 && rb->record_count % g_cleanup_interval == 0)
    {
        cleanup_old_records(rb, datatime);
    }
    
    fp = fopen(rb->filename, "ab");
    if (fp == NULL)
    {
        fprintf(stderr, "[RingClient] Failed to open %s\n", rb->filename);
        return -1;
    }
    
    if (fwrite(payload, 1, payloadlen, fp) != payloadlen)
    {
        fprintf(stderr, "[RingClient] Failed to write to %s\n", rb->filename);
        fclose(fp);
        return -1;
    }
    
    fclose(fp);
    
    rb->newest_time = datatime;
    if (rb->record_count == 0)
        rb->oldest_time = datatime;
    rb->record_count++;
    
    return 0;
}

static void 
ringbuffer_cleanup(void)
{
    RingBuffer *rb = ring_buffers;
    RingBuffer *next = NULL;
    
    while (rb != NULL)
    {
        next = rb->next;
        
        printf("[RingClient] Final: %s - %ld records, %.1f min\n",
               rb->streamid, rb->record_count,
               (rb->newest_time - rb->oldest_time) / 60.0);
        
        free(rb);
        rb = next;
    }
    
    ring_buffers = NULL;
}

static int
load_stream_file(SLCD *slconn, const char *streamfile)
{
    FILE *fp;
    char line[200];
    char stationid[64] = {0};
    char selector_str[200] = {0};
    char *cp;
    int fields;
    
    fp = fopen(streamfile, "rb");
    if (fp == NULL)
    {
        fprintf(stderr, "[RingClient] Cannot open stream file: %s\n", streamfile);
        return -1;
    }
    
    if (g_verbose >= 1)
        printf("[RingClient] Loading streams from: %s\n", streamfile);
    
    while (fgets(line, sizeof(line), fp))
    {
        memset(stationid, 0, sizeof(stationid));
        memset(selector_str, 0, sizeof(selector_str));
        
        if ((cp = strchr(line, '\r')) != NULL || (cp = strchr(line, '\n')) != NULL)
            *cp = '\0';
        
        fields = sscanf(line, "%63s %199[^\r\n]", stationid, selector_str);
        
        if (fields <= 0 || stationid[0] == '#')
            continue;
        
        if (fields == 2)
        {
            char *sel_copy = strdup(selector_str);
            char *token = strtok(sel_copy, " \t");
            while (token != NULL)
            {
                add_subscription(stationid, token);
                token = strtok(NULL, " \t");
            }
            free(sel_copy);
        }
        else
        {
            add_subscription(stationid, "");
        }
    }
    
    fclose(fp);
    
    return sl_add_streamlist_file(slconn, streamfile, NULL);
}

static void
packet_handler(SLCD *slconn, const SLpacketinfo *packetinfo,
               const char *payload, uint32_t payloadlength)
{
    char streamid[64];
    char loc_channel[16] = {0};
    char selector[16] = {0};
    const char *matched_selector;
    RingBuffer *rb = NULL;
    double datatime;

    (void)slconn;

    /* Check if we should stop */
    if (g_running_ptr && !(*g_running_ptr))
        return;

    if (packetinfo->stationid[0] == '\0')
        return;

    strncpy(streamid, packetinfo->stationid, sizeof(streamid) - 1);
    streamid[sizeof(streamid) - 1] = '\0';
    
    if (payloadlength >= 18)
    {
        extract_selector_from_miniseed(payload, loc_channel, sizeof(loc_channel));
        matched_selector = find_matching_selector(streamid, loc_channel);
        strncpy(selector, matched_selector, sizeof(selector) - 1);
        selector[sizeof(selector) - 1] = '\0';
    }
    else
    {
        return;
    }
    
    rb = get_or_create_ringbuffer(streamid, selector);
    if (rb == NULL)
        return;
    
    datatime = extract_miniseed_time(payload);
    
    if (write_packet_to_ringbuffer(rb, payload, payloadlength, datatime) == 0)
    {
        /* 
         * Verbose level behavior:
         * 0 = quiet, no per-packet output
         * 1 = normal, status every 100 packets
         * 2+ = debug, every packet
         */
        if (g_verbose >= 2)
        {
            /* Debug mode: show EVERY packet */
            time_t t = (time_t)datatime;
            struct tm *tm_info = gmtime(&t);
            char time_str[32];
            strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", tm_info);
            
            printf("[RingClient] PKT %s_%s seq=%llu time=%s bytes=%u\n",
                   streamid, loc_channel, 
                   (unsigned long long)packetinfo->seqnum,
                   time_str, payloadlength);
        }
        else if (g_verbose == 1)
        {
            /* Normal mode: show summary every 100 packets */
            static int count = 0;
            if (++count % 100 == 0)
            {
                printf("[RingClient] %s_%s: %ld records, %.1f min buffer\n",
                       streamid, selector, rb->record_count,
                       (rb->newest_time - rb->oldest_time) / 60.0);
            }
        }
        /* verbose == 0: no output */
    }
}