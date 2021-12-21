#include "co2_control.h"

#include <esp_log.h>

#include "rf_transmitter.h"
#include "nvs_namespace_keys.h"
#include "sensor.h"
#include "scd30_reading.h"

struct sensor_control* get_co2_control() { return &co2_control; }

void check_co2() {
    int result = control_check_sensor(&co2_control, sensor_get_value(get_co2_sensor()));
    if(!is_co2_control_on && result == -1) {
        inject_co2();
        is_co2_control_on = true;
    } else if(is_co2_control_on && result == 0) {
        stop_co2_adjustment();
        is_co2_control_on = false;
    }
}

void inject_co2() {
    ESP_LOGI(CO2_TAG, "Injecting CO2");
    control_power_outlet(CO2_INJECTION, true);
}

void stop_co2_adjustment() {
    ESP_LOGI(CO2_TAG, "Turning off CO2 injector");
    control_power_outlet(CO2_INJECTION, false);
}

void co2_update_settings(cJSON *item) {
    nvs_handle_t *handle = nvs_get_handle(CO2_NVS_NAMESPACE);
	control_update_settings(&co2_control, item, handle);

	nvs_commit_data(handle);
	ESP_LOGI(CO2_TAG, "Updated settings and committed data to NVS");
}

void co2_get_nvs_settings() {
	control_get_nvs_settings(&co2_control, CO2_NVS_NAMESPACE);
	ESP_LOGI(CO2_TAG ,"Updated settings from NVS");
}