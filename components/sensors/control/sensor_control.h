/**
 * TODO add description
 */

#define NUM_CHECKS 6

#include <stdbool.h>
#include <cjson.h>
#include <nvs.h>

#include "rtc.h"
#include "nvs_manager.h"

#ifndef COMPONENTS_SENSORS_CONTROL_SENSOR_CONTROL_H_
#define COMPONENTS_SENSORS_CONTROL_SENSOR_CONTROL_H_

// TODO separate out struct vars
struct sensor_control {
	char name[25];
	cJSON *status_object;
	bool is_control_enabled;
	bool is_control_active;
	bool is_doser;
	float target_value;
	float margin_error;
	bool is_up_control;
	bool is_down_control;
	bool sensor_checks[NUM_CHECKS];
	int check_index;
	struct timer dose_timer;
	struct timer wait_timer;
	float dose_time;
	float wait_time;
	float dose_percentage;
};

#endif /* COMPONENTS_SENSORS_CONTROL_SENSOR_CONTROL_H_ */

// TODO add RME's

// Initialize control structure
void init_sensor_control(struct sensor_control *control_in, char *name_in, cJSON *object_in, float margin_error_in);
void init_doser_control(struct sensor_control *control_in);

// Get enable/active statuses
bool control_get_enabled(struct sensor_control *control_in);
bool control_get_active(struct sensor_control *control_in);

// Get timers
struct timer* control_get_dose_timer(struct sensor_control *control_in);
struct timer* control_get_wait_timer(struct sensor_control *control_in);

// Enable and disable control
void control_enable(struct sensor_control *control_in);
void control_disable(struct sensor_control *control_in);

// Checks if sensor is out of range
bool control_is_under_target(struct sensor_control *control_in, float current_value);
bool control_is_over_target(struct sensor_control *control_in, float current_value);

// Checks sensor value and updates checks accordingly
// Returns 0 if sensor is fine, -1 if confirmed too low, and 1 if confirmed too high
int control_check_sensor(struct sensor_control *control_in, float current_value);

// Deal with dosing and waiting
void control_start_dose_timer(struct sensor_control *control_in);
void control_start_wait_timer(struct sensor_control *control_in);
void control_set_dose_percentage(struct sensor_control *control_in, float value);
float control_get_dose_time(struct sensor_control *control_in);

// Update settings using JSON string
void control_update_settings(struct sensor_control *control_in, cJSON *item, nvs_handle_t *handle);

// Get sensor settings stored in NVS
void control_get_nvs_settings(struct sensor_control *control_in, char *namespace);
