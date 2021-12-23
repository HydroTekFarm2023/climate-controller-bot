

#include <stdbool.h>
#include <cjson.h>

#include "sensor_control.h"

#define CO2_TAG "CO2_CONTROL"

// Margin of error
static const float CO2_MARGIN_ERROR = 10;

// Control struct
struct sensor_control co2_control;

// Track if co2 injector is on
bool is_co2_control_on;

// Get control
struct sensor_control* get_co2_control();

// Checks and adjust co2 level
void check_co2();

// inject co2 
void inject_co2();


// Turn co2 pump off
void stop_co2_adjustment();

// Update settings
void co2_update_settings(cJSON *item);

// Get and store settings from NVS
void co2_get_nvs_settings();


