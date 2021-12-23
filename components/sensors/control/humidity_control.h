#include <stdbool.h>
#include <cjson.h>

#include "sensor_control.h"

#define HUMIDITY_TAG "HUMIDITY_CONTROL"

// Margin of error
static const float HUMIDITY_MARGIN_ERROR = 5;

// Control struct
struct sensor_control humidity_control;

// Track if humidifier is on
bool is_humidity_control_on;

// Get control
struct sensor_control* get_humidity_control();

// Checks and adjust humidity level
void check_humidity();

// increase_humidity
void increase_humidity();

//decrease humidity
void decrease_humidity();

// Turn humidifier off
void stop_humidity_adjustment();

// Update settings
void humidity_update_settings(cJSON *item);

// Get and store settings from NVS
void humidity_get_nvs_settings();