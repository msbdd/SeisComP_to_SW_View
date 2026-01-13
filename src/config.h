#ifndef CONFIG_H
#define CONFIG_H

#define PACKAGE "SeisComP_To_SW_View"
#define VERSION "0.0.1"

#define MAX_CONFIG_STRING 256
#define MAX_CONFIG_PATH 512

#include <stddef.h>

#ifdef _WIN32
    #include <string.h>
    #define strcasecmp _stricmp
#endif

/* Main application configuration */
typedef struct {
    /* SeedLink server settings */
    char seedlink_server[MAX_CONFIG_STRING];
    int seedlink_port;
    char stream_file[MAX_CONFIG_PATH];
    int verbose;
    int ring_buffer_minutes;
    char state_file[MAX_CONFIG_PATH];
    int cleanup_interval;  /* Clean old records every N packets */
    
    /* Database settings for pick fetcher */
    int pickfetcher_enabled;
    char db_host[MAX_CONFIG_STRING];
    int db_port;
    char db_user[MAX_CONFIG_STRING];
    char db_password[MAX_CONFIG_STRING];
    char db_name[MAX_CONFIG_STRING];
    char picks_file[MAX_CONFIG_PATH];
    int picks_update_interval;
    int picks_lookback;
    
    /* Output directory */
    char output_dir[MAX_CONFIG_PATH];
} AppConfig;

/* Initialize configuration with defaults */
void config_init_defaults(AppConfig *config);

/* Load configuration from file */
int config_load(AppConfig *config, const char *filepath);

/* Print configuration (for debugging) */
void config_print(const AppConfig *config);

/* Validate configuration */
int config_validate(const AppConfig *config);

#endif /* CONFIG_H */