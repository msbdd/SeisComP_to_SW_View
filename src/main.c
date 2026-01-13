/*
 * Main application
 */
#ifdef _WIN32
    #include <winsock2.h>
    #include <windows.h>
#else
    #include <signal.h>
    #include <unistd.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "config.h"
#include "ringclient.h"
#include "pick_fetcher.h"

#define DEFAULT_CONFIG_FILE "config.txt"

/* Global flag for graceful shutdown */
static volatile int g_running = 1;

/* Pointers to configs so signal handler can stop both threads */
static RingClientConfig *g_rc_config = NULL;
static PickFetcherConfig *g_pf_config = NULL;

/* Signal handler for graceful shutdown */
#ifdef _WIN32
static BOOL WINAPI console_handler(DWORD signal) {
    if (signal == CTRL_C_EVENT || signal == CTRL_BREAK_EVENT) {
        printf("\n[Main] Shutdown signal received (Ctrl+C)...\n");
        
        /* Stop all threads */
        g_running = 0;
        
        if (g_rc_config) {
            g_rc_config->running = 0;
        }
        if (g_pf_config) {
            g_pf_config->running = 0;
        }
        
        return TRUE;
    }
    return FALSE;
}
#else
static void signal_handler(int sig) {
    (void)sig;
    printf("\n[Main] Shutdown signal received...\n");
    
    g_running = 0;
    
    if (g_rc_config) {
        g_rc_config->running = 0;
    }
    if (g_pf_config) {
        g_pf_config->running = 0;
    }
}
#endif

static void print_usage(const char *progname) {
    printf("\nUsage: %s [config_file]\n\n", progname);
    printf("  config_file   Path to configuration file (default: %s)\n\n", 
           DEFAULT_CONFIG_FILE);
    printf("Example config.txt:\n");
    printf("  # SeedLink settings\n");
    printf("  seedlink_server = geofon.gfz-potsdam.de\n");
    printf("  seedlink_port = 18000\n");
    printf("  stream_file = streams.txt\n");
    printf("  verbose = 1\n");
    printf("  ring_buffer_minutes = 5\n");
    printf("  cleanup_interval = 100\n");
    printf("  output_dir = ./data\n");
    printf("\n");
    printf("  # Pick fetcher settings\n");
    printf("  pickfetcher_enabled = true\n");
    printf("  db_host = 192.168.100.193\n");
    printf("  db_port = 3306\n");
    printf("  db_user = sysop\n");
    printf("  db_password = sysop\n");
    printf("  db_name = seiscomp\n");
    printf("  picks_file = picks.txt\n");
    printf("  picks_update_interval = 60\n");
    printf("  picks_lookback = 7200\n");
    printf("\n");
}

int main(int argc, char **argv) {
    AppConfig config;
    RingClientConfig rc_config;
    PickFetcherConfig pf_config;
    RingClientThread rc_thread;
    PickFetcherThread pf_thread;
    const char *config_file = DEFAULT_CONFIG_FILE;
    int rc_started = 0;
    int pf_started = 0;

    /* Parse command line */
    if (argc > 1) {
        if (strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        }
        config_file = argv[1];
    }

    printf("=== SeisComP_To_SW_View Data Client v%s ===\n\n", VERSION);

    /* Load configuration */
    printf("Loading configuration from: %s\n", config_file);
    if (config_load(&config, config_file) < 0) {
        fprintf(stderr, "Failed to load configuration\n");
        return 1;
    }

    /* Validate configuration */
    if (config_validate(&config) < 0) {
        fprintf(stderr, "Configuration validation failed\n");
        return 1;
    }

    /* Print configuration */
    config_print(&config);

    /* Initialize RingClient configuration */
    ringclient_init_config(&rc_config);
    strncpy(rc_config.server_address, config.seedlink_server, 
            sizeof(rc_config.server_address) - 1);
    rc_config.port = config.seedlink_port;
    strncpy(rc_config.stream_file, config.stream_file, 
            sizeof(rc_config.stream_file) - 1);
    strncpy(rc_config.state_file, config.state_file, 
            sizeof(rc_config.state_file) - 1);
    strncpy(rc_config.output_dir, config.output_dir, 
            sizeof(rc_config.output_dir) - 1);
    rc_config.verbose = config.verbose;
    rc_config.ring_buffer_minutes = config.ring_buffer_minutes;
    rc_config.cleanup_interval = config.cleanup_interval;

    /* Set global pointer for signal handler */
    g_rc_config = &rc_config;

    /* Initialize PickFetcher configuration if enabled */
    if (config.pickfetcher_enabled) {
        memset(&pf_config, 0, sizeof(pf_config));
        strncpy(pf_config.db_host, config.db_host, sizeof(pf_config.db_host) - 1);
        pf_config.db_port = config.db_port;
        strncpy(pf_config.db_user, config.db_user, sizeof(pf_config.db_user) - 1);
        strncpy(pf_config.db_password, config.db_password, 
                sizeof(pf_config.db_password) - 1);
        strncpy(pf_config.db_name, config.db_name, sizeof(pf_config.db_name) - 1);
        
        /* Build full path for picks file */
        if (config.output_dir[0] != '\0' && 
            config.picks_file[0] != '/' && 
            config.picks_file[0] != '\\' &&
            !(config.picks_file[1] == ':')) {
            snprintf(pf_config.output_filepath, sizeof(pf_config.output_filepath),
                     "%s/%s", config.output_dir, config.picks_file);
        } else {
            strncpy(pf_config.output_filepath, config.picks_file,
                    sizeof(pf_config.output_filepath) - 1);
        }
        
        pf_config.update_interval_sec = config.picks_update_interval;
        pf_config.lookback_sec = config.picks_lookback;
        
        /* Set global pointer for signal handler */
        g_pf_config = &pf_config;
    }

    /* Set up signal handlers BEFORE starting threads */
#ifdef _WIN32
    SetConsoleCtrlHandler(console_handler, TRUE);
#else
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
#endif

    /* Start RingClient thread */
    printf("[Main] Starting RingClient...\n");
    if (ringclient_start(&rc_config, &rc_thread) < 0) {
        fprintf(stderr, "[Main] Failed to start RingClient\n");
        return 1;
    }
    rc_started = 1;

    /* Start PickFetcher thread if enabled */
    if (config.pickfetcher_enabled) {
        printf("[Main] Starting PickFetcher...\n");
        if (pickfetcher_start(&pf_config, &pf_thread) < 0) {
            fprintf(stderr, "[Main] Warning: Failed to start PickFetcher\n");
        } else {
            pf_started = 1;
        }
    }

    printf("\n[Main] Running... Press Ctrl+C to stop.\n\n");

    /* Main loop - just wait for shutdown signal */
    while (g_running && rc_config.running) {
#ifdef _WIN32
        Sleep(1000);  /* Check every second */
#else
        sleep(1);
#endif
    }

    /* Shutdown */
    printf("\n[Main] Initiating shutdown...\n");

    /* Signal all threads to stop (in case signal handler didn't) */
    rc_config.running = 0;
    if (pf_started) {
        pf_config.running = 0;
    }

    /* Wait for threads to finish */
    if (pf_started) {
        printf("[Main] Waiting for PickFetcher to stop...\n");
        pickfetcher_stop(&pf_config, pf_thread);
        printf("[Main] PickFetcher stopped\n");
    }

    if (rc_started) {
        printf("[Main] Waiting for RingClient to stop...\n");
        ringclient_stop(&rc_config, rc_thread);
        printf("[Main] RingClient stopped\n");
    }

    /* Clear global pointers */
    g_rc_config = NULL;
    g_pf_config = NULL;

    printf("[Main] Shutdown complete.\n");
    return 0;
}