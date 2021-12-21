#include <stdbool.h>

#define SETTINGS_RECEIVED_KEY "SET_REC"
#define GROW_ACTIVE_KEY "GR_ACTV"

#define GROW_MANAGER_TAG "GROW_MANAGER"

// Vars to track status of settings and grow cycle
bool is_settings_received;
bool is_grow_active;

// Sets booleans based on NVS data and reacts accordingly
void init_grow_manager();

// Start/stop grow cycle
void start_grow_cycle();
void stop_grow_cycle();

// Function called when JSON settings are received through MQTT
void settings_received();

// Get boolean statuses
bool get_is_settings_received();
bool get_is_grow_active();
