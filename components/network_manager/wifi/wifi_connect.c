#include "wifi_connect.h"

#include <esp_err.h>
#include <esp_log.h>
#include <esp_event.h>
#include <esp_wifi.h>
#include <string.h>
#include <driver/gpio.h>
#include "ports.h"

#include "network_settings.h"

static void wifi_event_handler(void *arg, esp_event_base_t event_base,		// WiFi Event Handler
		int32_t event_id, void *event_data) {
	const char *TAG = "Event_Handler";
	ESP_LOGI(TAG, "Event dispatched from event loop base=%s, event_id=%d\n",
			event_base, event_id);

	// Check Event Type
	if (event_base == IP_EVENT && event_id == IP_EVENT_STA_GOT_IP) {
		ip_event_got_ip_t *event = (ip_event_got_ip_t*) event_data;
		ESP_LOGI(TAG, "got IP:%s", ip4addr_ntoa(&event->ip_info.ip));
		retryNumber = 0;
		is_wifi_connected = true; 
		xEventGroupSetBits(wifi_event_group, WIFI_CONNECTED_BIT);
	} else if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_START) {
		ESP_ERROR_CHECK(esp_wifi_connect());
		retryNumber = 0;
	} else if (event_base == WIFI_EVENT
			&& event_id == WIFI_EVENT_STA_DISCONNECTED) {
		// Attempt Reconnection
		if (retryNumber < RETRYMAX) {
			esp_wifi_connect();
			retryNumber++;
		} else {
			xEventGroupSetBits(wifi_event_group, WIFI_FAIL_BIT);
		}
		is_wifi_connected = false;
		ESP_LOGI(TAG, "WIFI Connection Failed; Reconnecting....\n");
	}
}

bool connect_wifi() {
	const char *TAG = "WIFI";
	ESP_LOGI(TAG, "Starting connect");

	is_wifi_connected = false; 

	const wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
	wifi_event_group = xEventGroupCreate();
	ESP_ERROR_CHECK(esp_wifi_init(&cfg));
	ESP_ERROR_CHECK(esp_event_handler_register(WIFI_EVENT, ESP_EVENT_ANY_ID, &wifi_event_handler, NULL));
	ESP_ERROR_CHECK(esp_event_handler_register(IP_EVENT, IP_EVENT_STA_GOT_IP, &wifi_event_handler, NULL));
	ESP_ERROR_CHECK(esp_event_handler_register(WIFI_EVENT, WIFI_EVENT_STA_DISCONNECTED, &wifi_event_handler, NULL));
	ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));

	wifi_config_t wifi_config;
	memset(&wifi_config, 0, sizeof(wifi_config));
	strcpy((char*)(wifi_config.sta.ssid), get_network_settings()->wifi_ssid);
	strcpy((char*)(wifi_config.sta.password), get_network_settings()->wifi_pw);

	ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &wifi_config));
	ESP_ERROR_CHECK(esp_wifi_start());

	// Do not proceed until WiFi is connected
	EventBits_t sta_event_bits;
	sta_event_bits = xEventGroupWaitBits(wifi_event_group, WIFI_CONNECTED_BIT | WIFI_FAIL_BIT, pdFALSE, pdFALSE, portMAX_DELAY);

	// Return and log based on event bit
	if ((sta_event_bits & WIFI_CONNECTED_BIT) != 0) {
		ESP_LOGI(TAG,  "Connected");
		is_wifi_connected = true;
		return true;
	}
	if ((sta_event_bits & WIFI_FAIL_BIT) != 0) {
		ESP_LOGE(TAG, "Connection Failed");
	} else {
		ESP_LOGE(TAG, "Unexpected Event");
	}
	is_wifi_connected = false;
	return false;
}

bool get_is_wifi_connected() {
	return is_wifi_connected; 
}
