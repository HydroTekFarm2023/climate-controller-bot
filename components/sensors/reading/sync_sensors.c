#include "sync_sensors.h"

#include <freertos/FreeRTOS.h>
#include <freertos/task.h>
#include <esp_err.h>
#include <esp_log.h>
#include <esp_system.h>

#include "scd30_reading.h"
#include "sensor.h"

void set_sensor_sync_bits() {
	sensor_sync_bits = DELAY_BIT | SCD_BIT;
}

void sync_task(void *parameter) {				// Sensor Synchronization Task
	const char *TAG = "Sync_Task";
	EventBits_t returnBits;
	for (;;) {
		// Provide a minimum amount of time before event group round resets
		vTaskDelay(pdMS_TO_TICKS(10000));
		returnBits = xEventGroupSync(sensor_event_group, DELAY_BIT, sensor_sync_bits, pdMS_TO_TICKS(MEASUREMENT_INTERVAL));
		// Check Whether all tasks were completed on time
		if ((returnBits & sensor_sync_bits) == sensor_sync_bits) {
			ESP_LOGI(TAG, "Completed\n");
		} else {
			ESP_LOGE(TAG, "Failed to Complete On Time\n");
		}
	}
}
