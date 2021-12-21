/*
 * network_settings.h
 *
 *  Created on: Dec 23, 2020
 *      Author: rnagp
 */

#ifndef COMPONENTS_NETWORK_MANAGER_NETWORK_SETTINGS_H_
#define COMPONENTS_NETWORK_MANAGER_NETWORK_SETTINGS_H_

// NVS keys for network data
#define INIT_PROPERTIES_KEY "INIT_PROPS"
#define WIFI_SSID_KEY "WIFI_SSID"
#define WIFI_PW_KEY "WIFI_PW"
#define DEVICE_ID_KEY "DEV_ID"
#define BROKER_IP_KEY "B_IP"

struct Network_Settings {
	char wifi_ssid[50];
	char wifi_pw[50];
	char broker_ip[20];
	char device_id[6];
};

#endif /* COMPONENTS_NETWORK_MANAGER_NETWORK_SETTINGS_H_ */

// Store network data
struct Network_Settings network_settings;

// Get struct
struct Network_Settings* get_network_settings();

// Initialize wifi and mqtt connections
void init_network_connections();

// Push/pull network settings to/from NVS
void push_network_settings();
void pull_network_settings();

// Set NVS flag that network settings are invalid
void set_invalid_network_settings();
