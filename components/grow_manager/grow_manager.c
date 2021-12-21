#include "grow_manager.h"

#include <esp_log.h>
#include <esp_err.h>
#include <FreeRTOS/freertos.h>
#include <freertos/task.h>

#include "nvs_manager.h"
#include "scd30_reading.h"
#include "nvs_namespace_keys.h"
#include "sync_sensors.h"
#include "mqtt_manager.h"
#include "control_task.h"
#include "rf_transmitter.h"
#include "rtc.h"
#include "co2_control.h"
#include "humidity_control.h"
#include "temperature_control.h"

void init_grow_manager() {
	uint8_t status; // Holds vars coming from NVS

	// Check for sensor settings status
	if(!nvs_get_uint8(GROW_SETTINGS_NVS_NAMESPACE, SETTINGS_RECEIVED_KEY, &status) || !status) {
		is_settings_received = false;
		stop_grow_cycle();
		return;
	} else {
		ESP_LOGI(GROW_MANAGER_TAG, "Settings stored in NVS");
		co2_get_nvs_settings();
		humidity_get_nvs_settings();
		temperature_get_nvs_settings();

		settings_received();
	}

	ESP_LOGI(GROW_MANAGER_TAG, "About to check");
	// Check for grow cycle status
	if(!nvs_get_uint8(GROW_SETTINGS_NVS_NAMESPACE, GROW_ACTIVE_KEY, &status) || !status) {
		ESP_LOGI(GROW_MANAGER_TAG, "About to stop cycle");
		stop_grow_cycle();
	} else {
		ESP_LOGI(GROW_MANAGER_TAG, "About to start cycle");
		start_grow_cycle();
	}
}

void push_grow_status() {
	// Store in NVS
	nvs_handle_t *handle = nvs_get_handle(GROW_SETTINGS_NVS_NAMESPACE);
	nvs_add_uint8(handle, GROW_ACTIVE_KEY, is_grow_active);
	nvs_commit_data(handle);
}

void push_grow_settings_status() {
	// Store in NVS
	nvs_handle_t *handle = nvs_get_handle(GROW_SETTINGS_NVS_NAMESPACE);
	nvs_add_uint8(handle, SETTINGS_RECEIVED_KEY, is_settings_received);
	nvs_commit_data(handle);
}

void suspend_tasks() {
	// Core 0 tasks
	vTaskSuspend(timer_alarm_task_handle);
	vTaskSuspend(publish_task_handle);
	vTaskSuspend(sensor_control_task_handle);

	// Core 1
	vTaskSuspend(*sensor_get_task_handle(get_temperature_sensor()));
	vTaskSuspend(sync_task_handle);
}

void resume_tasks() {
	// Core 0 tasks
	vTaskResume(timer_alarm_task_handle);
	vTaskResume(publish_task_handle);
	vTaskResume(sensor_control_task_handle);

	// Core 1
	vTaskResume(*sensor_get_task_handle(get_temperature_sensor()));
	vTaskResume(sync_task_handle);
}


void start_grow_cycle() {
	// Don't start unless settings have been received
	if(!is_settings_received){
		ESP_LOGE("Error", "Attempted to start grow cycle before sending settings");
		return;
	}

	// Set active to true and store in NVS
	is_grow_active = true;
	push_grow_status();

	resume_tasks();
	ESP_LOGI(GROW_MANAGER_TAG, "Started Grow Cycle");
}

void stop_grow_cycle() {
	// Set active to false ands store in NVS
	is_grow_active = false;
	push_grow_status();

	ESP_LOGI(GROW_MANAGER_TAG, "Stopped Grow Cycle");
	suspend_tasks();
}

void settings_received() {
	is_settings_received = true;
	push_grow_settings_status();
}

bool get_is_settings_received() { return is_settings_received; }
bool get_is_grow_active() { return is_grow_active; }
