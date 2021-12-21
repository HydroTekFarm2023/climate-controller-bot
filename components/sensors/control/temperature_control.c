#include "temperature_control.h"

#include <esp_log.h>

#include "rf_transmitter.h"
#include "nvs_namespace_keys.h"
#include "sensor.h"
#include "humidity_control.h"
#include "scd30_reading.h"

struct sensor_control* get_temperature_control() { return &temperature_control; }

void check_temperature() {
    if (!control_get_active(get_humidity_control())) {
            int result = control_check_sensor(&temperature_control, sensor_get_value(get_temperature_sensor()));
            if(!is_temperature_control_on && result == -1) {
                increase_temperature();
                is_temperature_control_on = true;
            } else if(!is_temperature_control_on && result == 1) {
                decrease_temperature();
                is_temperature_control_on = true;
             } else if(is_temperature_control_on && result == 0) {
                stop_temperature_adjustment();
                is_temperature_control_on = false;
             }
    }

}

void increase_temperature() {
    ESP_LOGI(TEMPERATURE_TAG, "Increasing Temperature");
    control_power_outlet(TEMPERATURE_HEATER, true);
}

void decrease_temperature() {
    ESP_LOGI(TEMPERATURE_TAG, "Decreasing Temperature");
    control_power_outlet(TEMPERATURE_COOLER, true);
}

void stop_temperature_adjustment() {
    ESP_LOGI(TEMPERATURE_TAG, "Turning off temperature control");
    control_power_outlet(TEMPERATURE_HEATER, false);
    control_power_outlet(TEMPERATURE_COOLER, false);
}

void temperature_update_settings(cJSON *item) {
    nvs_handle_t *handle = nvs_get_handle(TEMPERATURE_NVS_NAMESPACE);
	control_update_settings(&temperature_control, item, handle);

	nvs_commit_data(handle);
	ESP_LOGI(TEMPERATURE_TAG, "Updated settings and committed data to NVS");
}

void temperature_get_nvs_settings() {
	control_get_nvs_settings(&temperature_control, TEMPERATURE_NVS_NAMESPACE);
	ESP_LOGI(TEMPERATURE_TAG ,"Updated settings from NVS");
}