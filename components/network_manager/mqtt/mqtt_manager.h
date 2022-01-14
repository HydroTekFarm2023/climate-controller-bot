#ifndef __MQTT_MANAGER_H
#define __MQTT_MANAGER_H

#include <Freertos/freertos.h>
#include <Freertos/task.h>
#include <mqtt_client.h>
#include <cjson.h>
#include <string.h>

#include "rf_transmitter.h"
#include "ota.h"

// QOS settings
#define PUBLISH_DATA_QOS 1
#define SUBSCRIBE_DATA_QOS 2

#define DEVICE_TYPE "climate"

#define WIFI_CONNECT_HEADING "wifi_connect_status"
#define SENSOR_DATA_HEADING "live_data"
#define SENSOR_SETTINGS_HEADING "device_settings"
#define EQUIPMENT_STATUS_HEADING "equipment_status"
#define GROW_CYCLE_HEADING "device_status"
#define RF_CONTROL_HEADING "manual_rf_control"
#define OTA_UPDATE_HEADING "ota_update"
#define OTA_DONE_HEADING "ota_done"
#define VERSION_REQUEST_HEADING "version_request"
#define VERSION_RESULT_HEADING "version_result"

/**
 * OTA Result
 */
typedef enum {
    OTA_SUCCESS   = 0,//!< No alarms
    OTA_FAIL          //!< First alarm
} ota_result_t;

/**
 * OTA Failure Reason
 */
typedef enum {
    VERSION_NOT_FOUND = 0,
    INVALID_OTA_URL_RECEIVED,
    HTTP_CONNECTION_FAILED,
    OTA_UPDATE_FAILED,
    IMAGE_FILE_LARGER_THAN_OTA_PARTITION,
    OTA_WRTIE_OPERATION_FAILED,
    IMAGE_VALIDATION_FAILED,
    OTA_SET_BOOT_PARTITION_FAILED,
    NO_FALIURE
} ota_failure_reason_t;

#define TIME_STRING_LENGTH 21

#define MQTT_TAG "MQTT_MANAGER"

#ifdef __cplusplus
extern "C" {
#endif

// Task handle
TaskHandle_t publish_task_handle;

// MQTT client
esp_mqtt_client_handle_t mqtt_client;

// MQTT connect status
bool is_mqtt_connected;

// Topics
char *wifi_connect_topic;
char *sensor_data_topic;
char *sensor_settings_topic;
char *ota_update_topic;
char *ota_done_topic;
char *version_request_topic;
char *version_result_topic;
char *equipment_status_topic;
char *grow_cycle_topic;
char *rf_control_topic;

SemaphoreHandle_t mqtt_connect_semaphore;

// JSON objects for equipment status
cJSON *equipment_status_root;
cJSON *control_status_root;
cJSON *co2_control_status; 
cJSON *humidity_control_status; 
cJSON *temperature_control_status; 
cJSON *rf_status_root;
cJSON *rf_statuses[NUM_OUTLETS];


// Get JSON objects
cJSON *get_co2_control_status();
cJSON *get_humidity_control_status();
cJSON *get_temperature_control_status();
cJSON **get_rf_statuses();

// Set broker IP config in MQTT
void mqtt_connect();

// Initialize MQTT connection
void init_mqtt();

// Send mqtt message to publish sensor data to broker
void publish_sensor_data();

// Handle data recieved through subscribed topics
void data_handler(char *topic, uint32_t topic_len, char *data, uint32_t data_len);

// Initialize equipment data JSON
void init_equipment_status();

// Send equipment data over MQTT
void publish_equipment_status();

// Update system settings
void update_settings();

// Create publishing topic
void create_sensor_data_topic();

// Create settings data topic
void create_settings_data_topic();

// OTA result publish message
void publish_ota_result(esp_mqtt_client_handle_t client, ota_result_t ota_result, ota_failure_reason_t ota_failure_reason);

#ifdef __cplusplus
}
#endif

#endif