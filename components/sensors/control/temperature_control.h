#include <stdbool.h>
#include <cjson.h>

#include "sensor_control.h"

#define TEMPERATURE_TAG "TEMPERATURE_CONTROL"

// Margin of error (actual 1.5)
static const float TEMPERATURE_MARGIN_ERROR = 0.1;

// Control struct
struct sensor_control temperature_control;

// Track if thermostat is on
bool is_temperature_control_on;

// Get control
struct sensor_control* get_temperature_control();

// Checks and adjust temperature
void check_temperature();

// increase temp
void increase_temperature();

//decrease temp
void decrease_temperature();

// Turn thermostat off
void stop_temperature_adjustment();

// Update settings
void temperature_update_settings(cJSON *item);

// Get and store settings from NVS
void temperature_get_nvs_settings();