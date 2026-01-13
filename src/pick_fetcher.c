#include "pick_fetcher.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Helper function to format time_t to MySQL datetime string */
void format_mysql_datetime(time_t timestamp, char *buffer, size_t buffer_size) {
    struct tm *timeinfo = localtime(&timestamp);
    strftime(buffer, buffer_size, "%Y-%m-%d %H:%M:%S", timeinfo);
}

PickResult* get_picks(time_t start_time, time_t end_time, MYSQL *conn) {
    char start_time_str[32];
    char end_time_str[32];
    char query[1024];
    MYSQL_RES *result;
    MYSQL_ROW row;
    PickResult *pick_result = NULL;
    size_t count = 0;
    size_t capacity = 100;

    format_mysql_datetime(start_time, start_time_str, sizeof(start_time_str));
    format_mysql_datetime(end_time, end_time_str, sizeof(end_time_str));

    snprintf(query, sizeof(query),
        "SELECT "
        "Pick.waveformID_networkCode AS Network, "
        "Pick.waveformID_stationCode AS Station, "
        "Pick.waveformID_channelCode AS Channel, "
        "Pick.time_value AS PickTime, "
        "Pick.time_value_ms as PickTime_ms "
        "FROM Pick "
        "WHERE Pick.time_value > '%s' "
        "AND Pick.time_value < '%s'",
        start_time_str, end_time_str);

    if (mysql_query(conn, query)) {
        fprintf(stderr, "Query failed: %s\n", mysql_error(conn));
        return NULL;
    }

    result = mysql_store_result(conn);
    if (result == NULL) {
        fprintf(stderr, "mysql_store_result() failed: %s\n", mysql_error(conn));
        return NULL;
    }

    pick_result = (PickResult*)malloc(sizeof(PickResult));
    if (pick_result == NULL) {
        mysql_free_result(result);
        return NULL;
    }
    
    pick_result->picks = (PickData*)malloc(capacity * sizeof(PickData));
    if (pick_result->picks == NULL) {
        free(pick_result);
        mysql_free_result(result);
        return NULL;
    }
    pick_result->count = 0;

    while ((row = mysql_fetch_row(result))) {
        if (count >= capacity) {
            capacity *= 2;
            PickData *new_picks = (PickData*)realloc(pick_result->picks, capacity * sizeof(PickData));
            if (new_picks == NULL) {
                free_pick_result(pick_result);
                mysql_free_result(result);
                return NULL;
            }
            pick_result->picks = new_picks;
        }

        strncpy(pick_result->picks[count].network, row[0] ? row[0] : "", 15);
        pick_result->picks[count].network[15] = '\0';

        strncpy(pick_result->picks[count].station, row[1] ? row[1] : "", 15);
        pick_result->picks[count].station[15] = '\0';

        strncpy(pick_result->picks[count].channel, row[2] ? row[2] : "", 15);
        pick_result->picks[count].channel[15] = '\0';

        strncpy(pick_result->picks[count].pick_time, row[3] ? row[3] : "", 31);
        pick_result->picks[count].pick_time[31] = '\0';

        strncpy(pick_result->picks[count].pick_time_ms, row[4] ? row[4] : "", 15);
        pick_result->picks[count].pick_time_ms[15] = '\0';

        count++;
    }

    pick_result->count = count;
    mysql_free_result(result);

    return pick_result;
}

void free_pick_result(PickResult *result) {
    if (result) {
        free(result->picks);
        free(result);
    }
}

int write_picks_to_file(PickResult *picks, const char *filepath,
                        time_t start_time, time_t end_time) {
    char temp_filepath[520];
    FILE *file;

    snprintf(temp_filepath, sizeof(temp_filepath), "%s.tmp", filepath);

    file = fopen(temp_filepath, "w");
    if (file == NULL) {
        fprintf(stderr, "Failed to open file: %s\n", temp_filepath);
        return -1;
    }

    if (picks && picks->count > 0) {
        for (size_t i = 0; i < picks->count; i++) {
            int year, month, day, hour, min, sec;

            sscanf(picks->picks[i].pick_time, "%d-%d-%d %d:%d:%d",
                   &year, &month, &day, &hour, &min, &sec);

            int microseconds = atoi(picks->picks[i].pick_time_ms);

            fprintf(file, "%s, %s, %s, %04d-%02d-%02d %02d:%02d:%02d.%06d\n",
                    picks->picks[i].network,
                    picks->picks[i].station,
                    picks->picks[i].channel,
                    year, month, day, hour, min, sec, microseconds);
        }
    } else {
        char start_str[32], end_str[32];
        format_mysql_datetime(start_time, start_str, sizeof(start_str));
        format_mysql_datetime(end_time, end_str, sizeof(end_str));
        fprintf(file, "# No picks found from %s to %s\n", start_str, end_str);
    }

    fclose(file);

#ifdef _WIN32
    /* Windows: Use ReplaceFile for atomic replacement */
    if (!ReplaceFileA(filepath, temp_filepath, NULL, 
                      REPLACEFILE_IGNORE_MERGE_ERRORS, NULL, NULL)) {
        DWORD error = GetLastError();
        if (error == ERROR_FILE_NOT_FOUND) {
            if (rename(temp_filepath, filepath) != 0) {
                fprintf(stderr, "Failed to rename %s to %s\n", temp_filepath, filepath);
                remove(temp_filepath);
                return -1;
            }
        } else {
            fprintf(stderr, "ReplaceFile failed with error %lu\n", error);
            remove(temp_filepath);
            return -1;
        }
    }
#else
    if (rename(temp_filepath, filepath) != 0) {
        fprintf(stderr, "Failed to rename %s to %s\n", temp_filepath, filepath);
        remove(temp_filepath);
        return -1;
    }
#endif

    return 0;
}

/* Thread function that periodically fetches picks */
#ifdef _WIN32
static DWORD WINAPI pickfetcher_thread_func(LPVOID arg)
#else
static void* pickfetcher_thread_func(void *arg)
#endif
{
    PickFetcherConfig *config = (PickFetcherConfig*)arg;
    MYSQL *conn = NULL;
    
    printf("[PickFetcher] Thread started\n");
    printf("[PickFetcher] DB: %s@%s:%d/%s\n", 
           config->db_user, config->db_host, config->db_port, config->db_name);
    printf("[PickFetcher] Output: %s, Interval: %ds, Lookback: %ds\n",
           config->output_filepath, config->update_interval_sec, config->lookback_sec);

    /* Initialize MySQL connection */
    conn = mysql_init(NULL);
    if (conn == NULL) {
        fprintf(stderr, "[PickFetcher] mysql_init() failed\n");
#ifdef _WIN32
        return 1;
#else
        return NULL;
#endif
    }

    /* Set SSL options */
    my_bool verify = 0;
    mysql_optionsv(conn, MYSQL_OPT_SSL_VERIFY_SERVER_CERT, (void *)&verify);
    my_bool enforce_tls = 0;
    mysql_optionsv(conn, MYSQL_OPT_SSL_ENFORCE, (void *)&enforce_tls);

    /* Connect to database */
    if (mysql_real_connect(conn, config->db_host, config->db_user, 
                           config->db_password, config->db_name, 
                           config->db_port, NULL, 0) == NULL) {
        fprintf(stderr, "[PickFetcher] Connection failed: %s\n", mysql_error(conn));
        mysql_close(conn);
#ifdef _WIN32
        return 1;
#else
        return NULL;
#endif
    }

    printf("[PickFetcher] Connected to database\n");

    /* Main loop */
    while (config->running) {
        time_t end_time = time(NULL);
        time_t start_time = end_time - config->lookback_sec;

        PickResult *picks = get_picks(start_time, end_time, conn);

        if (picks) {
            printf("[PickFetcher] Found %zu picks\n", picks->count);
            
            if (write_picks_to_file(picks, config->output_filepath, 
                                    start_time, end_time) == 0) {
                printf("[PickFetcher] Updated %s\n", config->output_filepath);
            } else {
                fprintf(stderr, "[PickFetcher] Failed to write picks file\n");
            }

            free_pick_result(picks);
        } else {
            fprintf(stderr, "[PickFetcher] Failed to fetch picks, reconnecting...\n");
            
            /* Try to reconnect */
            mysql_close(conn);
            conn = mysql_init(NULL);
            if (conn) {
                mysql_optionsv(conn, MYSQL_OPT_SSL_VERIFY_SERVER_CERT, (void *)&verify);
                mysql_optionsv(conn, MYSQL_OPT_SSL_ENFORCE, (void *)&enforce_tls);
                if (mysql_real_connect(conn, config->db_host, config->db_user,
                                       config->db_password, config->db_name,
                                       config->db_port, NULL, 0) == NULL) {
                    fprintf(stderr, "[PickFetcher] Reconnection failed: %s\n", 
                            mysql_error(conn));
                }
            }
        }

        /* Sleep for the configured interval, checking running flag periodically */
        for (int i = 0; i < config->update_interval_sec && config->running; i++) {
#ifdef _WIN32
            Sleep(1000);  /* 1 second */
#else
            sleep(1);
#endif
        }
    }

    /* Cleanup */
    if (conn) {
        mysql_close(conn);
    }

    printf("[PickFetcher] Thread stopped\n");

#ifdef _WIN32
    return 0;
#else
    return NULL;
#endif
}

int pickfetcher_start(PickFetcherConfig *config, PickFetcherThread *thread) {
    config->running = 1;

#ifdef _WIN32
    *thread = CreateThread(NULL, 0, pickfetcher_thread_func, config, 0, NULL);
    if (*thread == NULL) {
        fprintf(stderr, "Failed to create pick fetcher thread\n");
        return -1;
    }
#else
    if (pthread_create(thread, NULL, pickfetcher_thread_func, config) != 0) {
        fprintf(stderr, "Failed to create pick fetcher thread\n");
        return -1;
    }
#endif

    return 0;
}

int pickfetcher_stop(PickFetcherConfig *config, PickFetcherThread thread) {
    config->running = 0;

#ifdef _WIN32
    WaitForSingleObject(thread, INFINITE);
    CloseHandle(thread);
#else
    pthread_join(thread, NULL);
#endif

    return 0;
}