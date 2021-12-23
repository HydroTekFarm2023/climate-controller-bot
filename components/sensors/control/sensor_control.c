#include "sensor_control.h"

#include <string.h>
#include <esp_log.h>
#include <esp_err.h>
#include "rtc.h"
#include "sync_sensors.h"
#include "control_settings_keys.h"

// --------------------------------------------------- Helper functions ----------------------------------------------

void control_reset_checks(struct sensor_control *control_in) {
	for(int i = 0; i < NUM_CHECKS; i++) control_in->sensor_checks[i] = false;
	control_in->check_index = 0;
}

bool control_add_check(struct sensor_control *control_in) {
	if(control_in->check_index == NUM_CHECKS) {
		if(control_in->is_doser) control_reset_checks(control_in);
		return true;
	}
	control_in->sensor_checks[control_in->check_index++] = true;
	ESP_LOGI(control_in->name, "Check %d done", control_in->check_index);
	return false;
}

float control_get_target_value(struct sensor_control *control_in) {
	return control_in->target_value;
}

// --------------------------------------------------------------------------------------------------------------------


// --------------------------------------------------- Public interface ----------------------------------------------

void init_sensor_control(struct sensor_control *control_in, char *name_in, cJSON *object_in, float margin_error_in) {
	strcpy(control_in->name, name_in);

	control_in->status_object = object_in;
	control_in->is_control_active = false;
	control_in->is_doser = false;
	control_in->margin_error = margin_error_in;

	control_reset_checks(control_in);

	ESP_LOGI(control_in->name, "Control initialized");
}
void init_doser_control(struct sensor_control *control_in) {
	control_in->is_doser = true;
	control_in->dose_percentage = 1.;

	ESP_LOGI(control_in->name, "Doser initialized");
}


bool control_get_enabled(struct sensor_control *control_in) { return control_in->is_control_enabled; }
bool control_get_active(struct sensor_control *control_in) { return control_in->is_control_active; }

struct timer* control_get_dose_timer(struct sensor_control *control_in) { return &control_in->dose_timer; }
struct timer* control_get_wait_timer(struct sensor_control *control_in) { return &control_in->wait_timer; }

void control_enable(struct sensor_control *control_in) {
	control_in->is_control_enabled = true;
	ESP_LOGI(control_in->name, "Enabled");
}
void control_disable(struct sensor_control *control_in) {

	control_in->is_control_enabled = false;
	control_in->is_control_active = false;
	control_in->dose_timer.active = false;
	control_in->wait_timer.active = false;

	//TODO turn off pumps if possible/ensure pumps are turned off (if doser)
	control_reset_checks(control_in);

	ESP_LOGI(control_in->name, "Disabled");
}

bool control_is_under_target(struct sensor_control *control_in, float current_value) {
	return current_value < (control_get_target_value(control_in) - control_in->margin_error);
}
bool control_is_over_target(struct sensor_control *control_in, float current_value) {
	return current_value > (control_get_target_value(control_in) + control_in->margin_error);
}

int control_check_sensor(struct sensor_control *control_in, float current_value) {
	if(!control_in->is_control_enabled) return 0;
	if(control_in->is_control_active) {
		if(control_in->is_doser && (control_in->dose_timer.active || control_in->wait_timer.active)) return 0;
	}

	bool under_target = control_in->is_up_control && control_is_under_target(control_in, current_value);
	bool over_target = control_in->is_down_control && control_is_over_target(control_in, current_value);

	if(under_target || over_target) {
		if(control_add_check(control_in)) {
			control_in->is_control_active = true;
			return under_target ? -1 : 1;
		}
	} else if(control_in->check_index > 0) {
		if(control_in->is_doser) control_in->is_control_active = false;
		control_reset_checks(control_in);
	}

	if(!control_in->is_doser) control_in->is_control_active = false;
	return 0;
}

void control_start_dose_timer(struct sensor_control *control_in) { enable_timer(&dev, &control_in->dose_timer, control_get_dose_time(control_in)); }
void control_start_wait_timer(struct sensor_control *control_in) { enable_timer(&dev, &control_in->wait_timer, control_in->wait_time - NUM_CHECKS * (SENSOR_MEASUREMENT_PERIOD / 1000)); }
void control_set_dose_percentage(struct sensor_control *control_in, float value) { control_in->dose_percentage = value; }
float control_get_dose_time(struct sensor_control *control_in) { return control_in->dose_time * control_in->dose_percentage; }

void control_update_settings(struct sensor_control *control_in, cJSON *item, nvs_handle_t *handle) {
	cJSON *element = item->child;
	while(element != NULL) {
		char *key = element->string;
		if(strcmp(key, MONITORING_ONLY) == 0) {
			!element->valueint ? control_enable(control_in) : control_disable(control_in);
			nvs_add_uint8(handle, MONITORING_ONLY, (uint8_t)(control_in->is_control_enabled));
			ESP_LOGI(control_in->name, "Updated control only to: %s", element->valueint != 0 ? "false" : "true");
		} else if(strcmp(key, CONTROL) == 0) {
			cJSON *control_element = element->child;
			while(control_element != NULL) {
				char *control_key = control_element->string;
				if(strcmp(control_key, DOSING_TIME) == 0) {
					control_in->dose_time = control_element->valuedouble;
					nvs_add_float(handle, DOSING_TIME, control_in->dose_time);
					ESP_LOGI(control_in->name, "Updated dosing time to: %f", control_element->valuedouble);
				} else if(strcmp(control_key, DOSING_INTERVAL) == 0) {
					control_in->wait_time = control_element->valuedouble;
					nvs_add_float(handle, DOSING_INTERVAL, control_in->wait_time);
					ESP_LOGI(control_in->name, "Updated wait time to: %f", control_element->valuedouble);
				} else if(strcmp(control_key, TARGET_VALUE) == 0) {
					control_in->target_value = control_element->valuedouble;
					nvs_add_float(handle, TARGET_VALUE, control_in->target_value);
					ESP_LOGI(control_in->name, "Updated target value to: %f", control_element->valuedouble);
				} else if(strcmp(control_key, UP_CONTROL) == 0) {
					control_in->is_up_control = control_element->valueint;
					nvs_add_uint8(handle, UP_CONTROL, control_element->valueint);
					ESP_LOGI(control_in->name, "Updated up control status to: %s", control_element->valueint ? "true" : "false");
				} else if(strcmp(control_key, DOWN_CONTROL) == 0) {
					control_in->is_down_control = control_element->valueint;
					nvs_add_uint8(handle, DOWN_CONTROL, control_element->valueint);
					ESP_LOGI(control_in->name, "Updated down control status to: %s", control_element->valueint ? "true" : "false");
				}
				control_element = control_element->next;
			}
		}
		// TODO add alarm functionality
		//else if(strcmp(key, ALARM_MIN) == 0) {
//			//TODO update alarm min
//			ESP_LOGI("Updated ph alarm min to: ", "%d", element->valueint);
//		} else if(strcmp(key, ALARM_MAX) == 0) {
//			//TODO update alarm min
//			ESP_LOGI("Updated ph alarm max to: ", "%d", element->valueint);
//		}

		element = element->next;
	}
	ESP_LOGI(control_in->name, "Finished updating all values");
}

void control_get_nvs_settings(struct sensor_control *control_in, char *namespace) {
	uint8_t enable_status;
	nvs_get_uint8(namespace, MONITORING_ONLY, (uint8_t*)(&enable_status));
	enable_status ? control_enable(control_in) : control_disable(control_in);

	nvs_get_float(namespace, TARGET_VALUE, &control_in->target_value);
	nvs_get_uint8(namespace, UP_CONTROL, (uint8_t*)(&control_in->is_up_control));
	nvs_get_uint8(namespace, DOWN_CONTROL, (uint8_t*)(&control_in->is_down_control));
	nvs_get_float(namespace, DOSING_TIME, &control_in->dose_time);
	nvs_get_float(namespace, DOSING_INTERVAL, &control_in->wait_time);
}

// --------------------------------------------------------------------------------------------------------------------
