#include <stdbool.h>
#include <string.h>
#include <esp_log.h>
#include <esp_err.h>
#include "sensor.h"


void init_sensor(struct sensor *sensor_in, char *name_in, bool active_in, bool calib_in) {
	strcpy(sensor_in->name, name_in);
	sensor_in->current_value = 0;
	sensor_in->is_active = active_in;
	sensor_in->is_calib = calib_in;
}

TaskHandle_t* sensor_get_task_handle(struct sensor *sensor_in) { return &sensor_in->task_handle; }

float sensor_get_value(const struct sensor *sensor_in) { return sensor_in->current_value; }
float* sensor_get_address_value(struct sensor *sensor_in) {	return &sensor_in->current_value; }
void sensor_set_value(struct sensor *sensor_in, float value) { sensor_in->current_value = value; }

bool sensor_get_active_status(struct sensor *sensor_in) { return sensor_in->is_active; }
void sensor_set_active_status(struct sensor *sensor_in, bool status) { sensor_in->is_active = status; }

bool sensor_calib_status(struct sensor *sensor_in) { return sensor_in->is_calib; }
void sensor_set_calib_status(struct sensor *sensor_in, bool status) { sensor_in->is_calib = status; }

void calibrate_sensor(struct sensor *sensor_in, esp_err_t (*calib_func)(i2c_dev_t*), i2c_dev_t *dev) {
	ESP_LOGI(sensor_in->name, "Start Calibration");

	int task_priority = uxTaskPriorityGet(&sensor_in->task_handle);
	vTaskPrioritySet(sensor_in->task_handle, (configMAX_PRIORITIES - 1));	// Temporarily increase priority so that calibration can take place without interruption

	esp_err_t error = (*calib_func)(dev); // Calibrate EC

	if (error != ESP_OK) {
		ESP_LOGE(sensor_in->name, "Calibration Failed: %d", error);
	} else {
		ESP_LOGI(sensor_in->name, "Calibration Success");
	}

	vTaskPrioritySet(&sensor_in->task_handle, task_priority);
}

void sensor_get_json(struct sensor *sensor_in, cJSON **obj) {
	*obj = cJSON_CreateObject();
	cJSON *name, *value;

	name = cJSON_CreateString(sensor_in->name);

	char value_str[8];
	snprintf(value_str, sizeof(value_str), "%.2f", sensor_in->current_value);
	value = cJSON_CreateString(value_str);

	cJSON_AddItemToObject(*obj, "name", name);
	cJSON_AddItemToObject(*obj, "value", value);
}
