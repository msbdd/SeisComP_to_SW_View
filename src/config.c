#include "config.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#ifdef _WIN32
    #define strcasecmp _stricmp
#endif

/* Helper: trim whitespace from both ends of a string */
static char* trim(char *str) {
    char *end;
    
    while (isspace((unsigned char)*str)) str++;
    
    if (*str == 0)
        return str;
    
    end = str + strlen(str) - 1;
    while (end > str && isspace((unsigned char)*end)) end--;
    
    end[1] = '\0';
    
    return str;
}

/* Helper: parse a boolean value */
static int parse_bool(const char *value) {
    if (strcasecmp(value, "true") == 0 ||
        strcasecmp(value, "yes") == 0 ||
        strcasecmp(value, "1") == 0 ||
        strcasecmp(value, "on") == 0) {
        return 1;
    }
    return 0;
}

void config_init_defaults(AppConfig *config) {
    memset(config, 0, sizeof(AppConfig));
    
    /* SeedLink defaults */
    strcpy(config->seedlink_server, "localhost");
    config->seedlink_port = 18000;
    strcpy(config->stream_file, "streams.txt");
    config->verbose = 0;
    config->ring_buffer_minutes = 5;
    config->state_file[0] = '\0';
    config->cleanup_interval = 100;  /* Clean every 100 packets by default */
    
    /* Database defaults */
    config->pickfetcher_enabled = 0;
    strcpy(config->db_host, "localhost");
    config->db_port = 3306;
    strcpy(config->db_user, "");
    strcpy(config->db_password, "");
    strcpy(config->db_name, "seiscomp");
    strcpy(config->picks_file, "picks.txt");
    config->picks_update_interval = 60;
    config->picks_lookback = 7200;
    
    /* Output */
    strcpy(config->output_dir, ".");
}

int config_load(AppConfig *config, const char *filepath) {
    FILE *fp;
    char line[1024];
    char *key, *value, *equals;
    int line_number = 0;
    
    fp = fopen(filepath, "r");
    if (fp == NULL) {
        fprintf(stderr, "Cannot open config file: %s\n", filepath);
        return -1;
    }
    
    config_init_defaults(config);
    
    while (fgets(line, sizeof(line), fp)) {
        line_number++;
        
        line[strcspn(line, "\r\n")] = '\0';
        
        key = trim(line);
        if (key[0] == '\0' || key[0] == '#' || key[0] == ';') {
            continue;
        }
        
        equals = strchr(key, '=');
        if (equals == NULL) {
            fprintf(stderr, "Warning: Invalid line %d: %s\n", line_number, line);
            continue;
        }
        
        *equals = '\0';
        value = trim(equals + 1);
        key = trim(key);
        
        /* SeedLink settings */
        if (strcasecmp(key, "seedlink_server") == 0) {
            strncpy(config->seedlink_server, value, MAX_CONFIG_STRING - 1);
        }
        else if (strcasecmp(key, "seedlink_port") == 0) {
            config->seedlink_port = atoi(value);
        }
        else if (strcasecmp(key, "stream_file") == 0) {
            strncpy(config->stream_file, value, MAX_CONFIG_PATH - 1);
        }
        else if (strcasecmp(key, "verbose") == 0) {
            config->verbose = atoi(value);
        }
        else if (strcasecmp(key, "ring_buffer_minutes") == 0) {
            config->ring_buffer_minutes = atoi(value);
        }
        else if (strcasecmp(key, "state_file") == 0) {
            strncpy(config->state_file, value, MAX_CONFIG_PATH - 1);
        }
        else if (strcasecmp(key, "cleanup_interval") == 0) {
            config->cleanup_interval = atoi(value);
        }
        
        /* Database settings */
        else if (strcasecmp(key, "pickfetcher_enabled") == 0) {
            config->pickfetcher_enabled = parse_bool(value);
        }
        else if (strcasecmp(key, "db_host") == 0) {
            strncpy(config->db_host, value, MAX_CONFIG_STRING - 1);
        }
        else if (strcasecmp(key, "db_port") == 0) {
            config->db_port = atoi(value);
        }
        else if (strcasecmp(key, "db_user") == 0) {
            strncpy(config->db_user, value, MAX_CONFIG_STRING - 1);
        }
        else if (strcasecmp(key, "db_password") == 0) {
            strncpy(config->db_password, value, MAX_CONFIG_STRING - 1);
        }
        else if (strcasecmp(key, "db_name") == 0) {
            strncpy(config->db_name, value, MAX_CONFIG_STRING - 1);
        }
        else if (strcasecmp(key, "picks_file") == 0) {
            strncpy(config->picks_file, value, MAX_CONFIG_PATH - 1);
        }
        else if (strcasecmp(key, "picks_update_interval") == 0) {
            config->picks_update_interval = atoi(value);
        }
        else if (strcasecmp(key, "picks_lookback") == 0) {
            config->picks_lookback = atoi(value);
        }
        
        /* Output settings */
        else if (strcasecmp(key, "output_dir") == 0) {
            strncpy(config->output_dir, value, MAX_CONFIG_PATH - 1);
        }
        
        else {
            fprintf(stderr, "Warning: Unknown config key '%s' on line %d\n", 
                    key, line_number);
        }
    }
    
    fclose(fp);
    return 0;
}

void config_print(const AppConfig *config) {
    printf("=== Configuration ===\n");
    printf("\n[SeedLink]\n");
    printf("  server:            %s:%d\n", config->seedlink_server, config->seedlink_port);
    printf("  stream_file:       %s\n", config->stream_file);
    printf("  verbose:           %d", config->verbose);
    if (config->verbose == 0) printf(" (quiet)\n");
    else if (config->verbose == 1) printf(" (normal)\n");
    else if (config->verbose >= 2) printf(" (debug - per-packet info)\n");
    else printf("\n");
    printf("  ring_buffer_min:   %d\n", config->ring_buffer_minutes);
    printf("  cleanup_interval:  %d packets\n", config->cleanup_interval);
    printf("  state_file:        %s\n", 
           config->state_file[0] ? config->state_file : "(none)");
    
    printf("\n[PickFetcher]\n");
    printf("  enabled:           %s\n", config->pickfetcher_enabled ? "yes" : "no");
    if (config->pickfetcher_enabled) {
        printf("  db_host:           %s:%d\n", config->db_host, config->db_port);
        printf("  db_user:           %s\n", config->db_user);
        printf("  db_name:           %s\n", config->db_name);
        printf("  picks_file:        %s\n", config->picks_file);
        printf("  update_interval:   %d sec\n", config->picks_update_interval);
        printf("  lookback:          %d sec\n", config->picks_lookback);
    }
    
    printf("\n[Output]\n");
    printf("  output_dir:        %s\n", config->output_dir);
    printf("=====================\n\n");
}

int config_validate(const AppConfig *config) {
    int errors = 0;
    
    if (config->seedlink_server[0] == '\0') {
        fprintf(stderr, "Error: seedlink_server is required\n");
        errors++;
    }
    
    if (config->seedlink_port <= 0 || config->seedlink_port > 65535) {
        fprintf(stderr, "Error: seedlink_port must be 1-65535\n");
        errors++;
    }
    
    if (config->ring_buffer_minutes <= 0) {
        fprintf(stderr, "Error: ring_buffer_minutes must be positive\n");
        errors++;
    }
    
    if (config->cleanup_interval <= 0) {
        fprintf(stderr, "Error: cleanup_interval must be positive\n");
        errors++;
    }
    
    if (config->pickfetcher_enabled) {
        if (config->db_host[0] == '\0') {
            fprintf(stderr, "Error: db_host is required when pickfetcher is enabled\n");
            errors++;
        }
        if (config->db_user[0] == '\0') {
            fprintf(stderr, "Error: db_user is required when pickfetcher is enabled\n");
            errors++;
        }
        if (config->picks_update_interval <= 0) {
            fprintf(stderr, "Error: picks_update_interval must be positive\n");
            errors++;
        }
    }
    
    return errors == 0 ? 0 : -1;
}