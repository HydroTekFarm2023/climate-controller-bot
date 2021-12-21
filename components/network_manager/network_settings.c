#include "network_settings.h"

#include <esp_err.h>
#include <esp_system.h>
#include <esp_event.h>
#include <esp_log.h>

#include "nvs_manager.h"
#include "nvs_namespace_keys.h"
#include "access_point.h"
#include "wifi_connect.h"
#include "mqtt_manager.h"

struct Network_Settings* get_network_settings() { return &network_settings; }

void init_network_connections() {
	const char *TAG = "INIT_NETWORK_PROPERTIES";

	// Check if initial properties haven't been initialized before
	uint8_t init_properties_status;
	if(!nvs_get_uint8(NETWORK_SETTINGS_NVS_NAMESPACE, INIT_PROPERTIES_KEY, &init_properties_status) || init_properties_status == 0) {
		ESP_LOGI(TAG, "Properties not initialized. Starting access point");

		do {
			// Creates access point for mobile connection to receive wifi SSID and pw, broker IP address, and station name
			init_access_point_mode();
		} while(!connect_wifi());

		push_network_settings();

	} else {
		pull_network_settings();
		connect_wifi();
	}

	// Initialize and connect to MQTT
	init_mqtt();
	mqtt_connect();

	ESP_LOGI(TAG, "Init properties done");
}

void push_network_settings() {
	uint8_t network_settings_status = 1;

	nvs_handle_t *handle = nvs_get_handle(NETWORK_SETTINGS_NVS_NAMESPACE);

	nvs_add_string(handle, WIFI_SSID_KEY, network_settings.wifi_ssid);
	nvs_add_string(handle, WIFI_PW_KEY, network_settings.wifi_pw);
	nvs_add_string(handle, DEVICE_ID_KEY, network_settings.device_id);
	nvs_add_string(handle, BROKER_IP_KEY, network_settings.broker_ip);
	nvs_add_uint8(handle, INIT_PROPERTIES_KEY, network_settings_status);

	nvs_commit_data(handle);
}

void pull_network_settings() {
	nvs_get_string(NETWORK_SETTINGS_NVS_NAMESPACE, WIFI_SSID_KEY, network_settings.wifi_ssid);
	nvs_get_string(NETWORK_SETTINGS_NVS_NAMESPACE, WIFI_PW_KEY, network_settings.wifi_pw);
	nvs_get_string(NETWORK_SETTINGS_NVS_NAMESPACE, DEVICE_ID_KEY, network_settings.device_id);
	nvs_get_string(NETWORK_SETTINGS_NVS_NAMESPACE, BROKER_IP_KEY, network_settings.broker_ip);
}

void set_invalid_network_settings() {
	uint8_t network_settings_status = 0;

	nvs_handle_t *handle = nvs_get_handle(NETWORK_SETTINGS_NVS_NAMESPACE);
	nvs_add_uint8(handle, INIT_PROPERTIES_KEY, network_settings_status);
	nvs_commit_data(handle);
}
