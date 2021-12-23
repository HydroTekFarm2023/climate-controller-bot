#include "humidity_control.h"

#include <esp_log.h>

#include "rf_transmitter.h"
#include "nvs_namespace_keys.h"
#include "temperature_control.h"
#include "sensor.h"
#include "scd30_reading.h"

struct sensor_control* get_humidity_control() { return &humidity_control; }

void check_humidity() {
    if (!control_get_active(get_temperature_control())) {
            int result = control_check_sensor(&humidity_control, sensor_get_value(get_humidity_sensor()));
            if(!is_humidity_control_on && result == -1) {
                increase_humidity();
                is_humidity_control_on = true;
             } else if(!is_humidity_control_on && result == 1) {
                decrease_humidity();
                is_humidity_control_on = true;
            } else if(is_humidity_control_on && result == 0) {
                stop_humidity_adjustment();
                is_humidity_control_on = false;
            }
    }
}

void increase_humidity() {
    ESP_LOGI(HUMIDITY_TAG, "Increasing Humidity");
    control_power_outlet(HUMIDIFIER, true);
}

void decrease_humidity() {
    ESP_LOGI(HUMIDITY_TAG, "Decreasing Humidity");
    control_power_outlet(DEHUMIDIFIER, true);
}

void stop_humidity_adjustment() {
    ESP_LOGI(HUMIDITY_TAG, "Turning off humidifier control");
    control_power_outlet(HUMIDIFIER, false);
    control_power_outlet(DEHUMIDIFIER, false);
}

void humidity_update_settings(cJSON *item) {
    nvs_handle_t *handle = nvs_get_handle(HUMIDITY_NVS_NAMESPACE);
	control_update_settings(&humidity_control, item, handle);

	nvs_commit_data(handle);
	ESP_LOGI(HUMIDITY_TAG, "Updated settings and committed data to NVS");
}

void humidity_get_nvs_settings() {
	control_get_nvs_settings(&humidity_control, HUMIDITY_NVS_NAMESPACE);
	ESP_LOGI(HUMIDITY_TAG ,"Updated settings from NVS");
}