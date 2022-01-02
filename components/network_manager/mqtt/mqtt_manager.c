#include "mqtt_manager.h"

#include <esp_event.h>
#include <esp_log.h>
#include <esp_err.h>
#include <freertos/semphr.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>

#include "boot.h"
#include "scd30_reading.h"
#include "co2_control.h"
#include "humidity_control.h"
#include "temperature_control.h"
#include "sync_sensors.h"
#include "rf_transmitter.h"
#include "rtc.h"
#include "network_settings.h"
#include "grow_manager.h"
#include "wifi_connect.h"

static void initiate_ota(const char *mqtt_data);
static esp_err_t parse_ota_parameters(const char *buffer, char *version, char *endpoint);
static esp_err_t validate_ota_parameters(char *version, char *endpoint);
static void publish_firmware_version();

extern char *url_buf;
extern bool is_ota_success_on_bootup;

void mqtt_event_handler(esp_mqtt_event_handle_t event) {
	const char *TAG = "MQTT_Event_Handler";
	switch (event->event_id) {
	case MQTT_EVENT_CONNECTED:
		ESP_LOGI(TAG, "Connected\n");
		xSemaphoreGive(mqtt_connect_semaphore);
		break;
	case MQTT_EVENT_DISCONNECTED:
		ESP_LOGI(TAG, "Disconnected\n");
		break;
	case MQTT_EVENT_SUBSCRIBED:
		ESP_LOGI(TAG, "Subscribed\n");
		break;
	case MQTT_EVENT_UNSUBSCRIBED:
		ESP_LOGI(TAG, "UnSubscribed\n");
		break;
	case MQTT_EVENT_PUBLISHED:
		ESP_LOGI(TAG, "Published\n");
		break;
	case MQTT_EVENT_DATA:
		ESP_LOGI(TAG, "Message received\n");
		data_handler(event->topic, event->topic_len, event->data, event->data_len);
		break;
	case MQTT_EVENT_ERROR:
		ESP_LOGI(TAG, "Error\n");
		break;
	case MQTT_EVENT_BEFORE_CONNECT:
		ESP_LOGI(TAG, "Before Connection\n");
		break;
	default:
		ESP_LOGI(TAG, "Other Command\n");
		break;
	}
}

void create_str(char** str, char* init_str) { // Create method to allocate memory and assign initial value to string
	*str = malloc(strlen(init_str) * sizeof(char)); // Assign memory based on size of initial value
	if(!(*str)) { // Restart if memory alloc fails
		ESP_LOGE("", "Memory allocation failed. Restarting ESP32");
		restart_esp32();
	}
	strcpy(*str, init_str); // Copy initial value into string
}
void append_str(char** str, char* str_to_add) { // Create method to reallocate and append string to already allocated string
	*str = realloc(*str, (strlen(*str) + strlen(str_to_add)) * sizeof(char) + 1); // Reallocate data based on existing and new string size
	if(!(*str)) { // Restart if memory alloc fails
		ESP_LOGE("", "Memory allocation failed. Restarting ESP32");
		restart_esp32();
	}
	strcat(*str, str_to_add); // Concatenate strings
}

// Add sensor data to JSON entry
void add_entry(char** data, bool* first, char* name, float num) {
	// Add a comma to the beginning of every entry except the first
	if(*first) *first = false;
	else append_str(data, ",");

	// Convert float data into string
	char value[8];
	snprintf(value, sizeof(value), "%.2f", num);

	// Create entry string
	char *entry = NULL;
	create_str(&entry, "{ \"name\": \"");

	// Create entry using key, value, and other JSON syntax
	append_str(&entry, name);
	append_str(&entry, "\", \"value\": \"");
	append_str(&entry, value);
	append_str(&entry, "\"}");

	// Add entry to overall JSON data
	append_str(data, entry);

	// Free allocated memory
	free(entry);
	entry = NULL;
}

void init_topic(char **topic, int topic_len, char *heading) {
	*topic = malloc(sizeof(char) * topic_len);
	strcpy(*topic, heading);
}

void add_id(char *topic) {
	strcat(topic, "/");
	strcat(topic, get_network_settings()->device_id);
}

void add_device_type(char *topic) {
   strcat(topic, "/");
   strcat(topic, DEVICE_TYPE);
}

void make_topics() {
	ESP_LOGI("", "Starting make topics");

	int device_id_len = strlen(get_network_settings()->device_id);
	int device_type_len = strlen(DEVICE_TYPE);

	init_topic(&wifi_connect_topic, device_id_len + 1 + strlen(WIFI_CONNECT_HEADING) + 1, WIFI_CONNECT_HEADING);
	add_id(wifi_connect_topic);
	ESP_LOGI(MQTT_TAG, "Wifi Topic: %s", wifi_connect_topic);

	init_topic(&sensor_data_topic, device_id_len + 1 + strlen(SENSOR_DATA_HEADING) + 1, SENSOR_DATA_HEADING);
	add_id(sensor_data_topic);
	ESP_LOGI(MQTT_TAG, "Sensor data topic: %s", sensor_data_topic);

	init_topic(&sensor_settings_topic, device_id_len + 1 + strlen(SENSOR_SETTINGS_HEADING) + 1, SENSOR_SETTINGS_HEADING);
	add_id(sensor_settings_topic);
	ESP_LOGI(MQTT_TAG, "Sensor settings topic: %s", sensor_settings_topic);

	init_topic(&equipment_status_topic, device_id_len + 1 + strlen(EQUIPMENT_STATUS_HEADING) + 1, EQUIPMENT_STATUS_HEADING);
	add_id(equipment_status_topic);
	ESP_LOGI(MQTT_TAG, "Equipment settings topic: %s", equipment_status_topic);

	init_topic(&grow_cycle_topic, device_id_len + 1 + strlen(GROW_CYCLE_HEADING) + 1, GROW_CYCLE_HEADING);
	add_id(grow_cycle_topic);
	ESP_LOGI(MQTT_TAG, "Grow Cycle topic: %s", grow_cycle_topic);

	init_topic(&rf_control_topic, device_id_len + 1 + strlen(RF_CONTROL_HEADING) + 1, RF_CONTROL_HEADING);
	add_id(rf_control_topic);
	ESP_LOGI(MQTT_TAG, "RF control settings topic: %s", rf_control_topic);

	init_topic(&ota_update_topic, device_type_len + 1 + strlen(OTA_UPDATE_HEADING) + 1, OTA_UPDATE_HEADING);
	add_device_type(ota_update_topic);
	ESP_LOGI(MQTT_TAG, "OTA update topic: %s", ota_update_topic);

	init_topic(&ota_done_topic, device_type_len + 1 + strlen(OTA_DONE_HEADING) + 1, OTA_DONE_HEADING);
	add_device_type(ota_done_topic);
	ESP_LOGI(MQTT_TAG, "OTA done topic: %s", ota_done_topic);

	init_topic(&version_request_topic, device_type_len + 1 + strlen(VERSION_REQUEST_HEADING) + 1, VERSION_REQUEST_HEADING);
	add_device_type(version_request_topic);
	ESP_LOGI(MQTT_TAG, "Version request topic: %s", version_request_topic);

	init_topic(&version_result_topic, device_type_len + 1 + strlen(VERSION_RESULT_HEADING) + 1, VERSION_RESULT_HEADING);
	add_device_type(version_result_topic);
	ESP_LOGI(MQTT_TAG, "Version result topic: %s", version_result_topic);
}

void subscribe_topics() {
	// Subscribe to topics
	esp_mqtt_client_subscribe(mqtt_client, sensor_settings_topic, SUBSCRIBE_DATA_QOS);
	esp_mqtt_client_subscribe(mqtt_client, grow_cycle_topic, SUBSCRIBE_DATA_QOS);
	esp_mqtt_client_subscribe(mqtt_client, rf_control_topic, SUBSCRIBE_DATA_QOS);
	esp_mqtt_client_subscribe(mqtt_client, ota_update_topic, SUBSCRIBE_DATA_QOS);
   	esp_mqtt_client_subscribe(mqtt_client, version_request_topic, SUBSCRIBE_DATA_QOS);
}

void init_mqtt() {
	// Set broker configuration
	esp_mqtt_client_config_t mqtt_cfg = {
			.host = get_network_settings()->broker_ip,
			.port = 1883,
			.event_handle = mqtt_event_handler
	};

	// Create MQTT client
	mqtt_client = esp_mqtt_client_init(&mqtt_cfg);

	// Dynamically create topics
	make_topics();

	// Create equipment status JSON
	init_equipment_status();
}

void mqtt_connect() {
	// First check if wifi is connected
	if(!is_wifi_connected) {
		is_mqtt_connected = false;
		return;
	}

	// Connect mqtt
	mqtt_connect_semaphore = xSemaphoreCreateBinary();
	esp_mqtt_client_start(mqtt_client);
	xSemaphoreTake(mqtt_connect_semaphore, portMAX_DELAY); // TODO add approximate time to connect to mqtt

	// Subscribe to topics
	subscribe_topics();

	// Send connect success message (must be retain message)
	esp_mqtt_client_publish(mqtt_client, wifi_connect_topic, "1", 0, PUBLISH_DATA_QOS, 1);

	// Send equipment statuses
	publish_equipment_status();

	is_mqtt_connected = true;
}


void create_time_json(cJSON **time_json) {
	char time_str[TIME_STRING_LENGTH];

	struct tm time;
	get_date_time(&time);

	sprintf(time_str, "%.4d", time.tm_year + 1900);
	strcat(time_str, "-");
	sprintf(time_str + 5, "%.2d", time.tm_mon);
	strcat(time_str, "-");
	sprintf(time_str + 8, "%.2d", time.tm_mday);
	strcat(time_str, "T");
	sprintf(time_str + 11, "%.2d", time.tm_hour);
	strcat(time_str, ":");
	sprintf(time_str + 14, "%.2d", time.tm_min);
	strcat(time_str, ":");
	sprintf(time_str + 17, "%.2d", time.tm_sec);
	strcat(time_str, "Z");

	*time_json = cJSON_CreateString(time_str);
}


void publish_sensor_data(void *parameter) {			// MQTT Setup and Data Publishing Task
	ESP_LOGI(MQTT_TAG, "Sensor data topic: %s", sensor_data_topic);

	for (;;) {
		if(!is_mqtt_connected) {
			ESP_LOGE(MQTT_TAG, "Wifi not connected, cannot send MQTT data");

			// Wait for delay period and try again
			vTaskDelay(pdMS_TO_TICKS(SENSOR_MEASUREMENT_PERIOD));
			continue;
		}

		cJSON *root, *time, *sensor_arr, *sensor;

		// Initializing json object and sensor array
		root = cJSON_CreateObject();
		sensor_arr = cJSON_CreateArray();

		// Adding time
		create_time_json(&time);
		cJSON_AddItemToObject(root, "time", time);

		//Adding co2
		sensor_get_json(get_co2_sensor(), &sensor);
		cJSON_AddItemToArray(sensor_arr, sensor);

		//Adding humidity
		sensor_get_json(get_humidity_sensor(), &sensor);
		cJSON_AddItemToArray(sensor_arr, sensor);

		//Adding temperature
		sensor_get_json(get_temperature_sensor(), &sensor);
		cJSON_AddItemToArray(sensor_arr, sensor);

		// Adding array to object
		cJSON_AddItemToObject(root, "sensors", sensor_arr);

		// Creating string from JSON
		char *data = cJSON_PrintUnformatted(root);

		// Free memory
		cJSON_Delete(root);

		// Publish data to MQTT broker using topic and data
		esp_mqtt_client_publish(mqtt_client, sensor_data_topic, data, 0, PUBLISH_DATA_QOS, 0);

		ESP_LOGI(MQTT_TAG, "Sensor data: %s", data);

		// Publish data every sensor reading
		vTaskDelay(pdMS_TO_TICKS(SENSOR_MEASUREMENT_PERIOD));
	}

	free(wifi_connect_topic);
	free(sensor_data_topic);
	free(sensor_settings_topic);
}

cJSON  *get_co2_control_status() { return co2_control_status; }
cJSON  *get_humidity_control_status() { return humidity_control_status; }
cJSON  *get_temperature_control_status() { return temperature_control_status; }
cJSON **get_rf_statuses() { return rf_statuses; }

void init_equipment_status() {
	equipment_status_root = cJSON_CreateObject();
	control_status_root = cJSON_CreateObject();
	rf_status_root = cJSON_CreateObject();

	// Create sensor statuses
	co2_control_status = cJSON_CreateNumber(0);
	temperature_control_status = cJSON_CreateNumber(0);
	humidity_control_status = cJSON_CreateNumber(0);
	cJSON_AddItemToObject(control_status_root, "co2_control", co2_control_status);
	cJSON_AddItemToObject(control_status_root, "temperature_control", temperature_control_status);
	cJSON_AddItemToObject(control_status_root, "humidity_control", humidity_control_status);
	// Create rf statuses
	char key[3];
	for(uint8_t i = 0; i < NUM_OUTLETS; ++i) {
		rf_statuses[i] = cJSON_CreateNumber(0);

		sprintf(key, "%d", i);
		cJSON_AddItemToObject(rf_status_root, key, rf_statuses[i]);
	}

	cJSON_AddItemToObject(equipment_status_root, "rf", rf_status_root);
	cJSON_AddItemToObject(equipment_status_root, "control", control_status_root);
}

void publish_equipment_status() {
	char *data = cJSON_Print(equipment_status_root); // Create data string
	esp_mqtt_client_publish(mqtt_client, equipment_status_topic, data, 0, PUBLISH_DATA_QOS, 1); // Publish data
	ESP_LOGI(MQTT_TAG, "Equipment Data: %s", data);
}

void update_settings(char *settings) {
	cJSON *root = cJSON_Parse(settings);
	char* string = cJSON_Print(root);
	ESP_LOGI(MQTT_TAG, "datavalue:\n %s\n", string);
	cJSON *object_settings = root->child;
	
	char *data_topic = object_settings->string;
	ESP_LOGI(MQTT_TAG, "datatopic: %s\n", data_topic);

	if(strcmp("co2", data_topic) == 0) {
			ESP_LOGI(MQTT_TAG, "co2 data received");
			co2_update_settings(object_settings);
	} else if(strcmp("air_temp", data_topic) == 0) {
			ESP_LOGI(MQTT_TAG, "temperature data received");
			temperature_update_settings(object_settings);
	} else if(strcmp("humidity", data_topic) == 0) {
			ESP_LOGI(MQTT_TAG, "humidity data received");
			humidity_update_settings(object_settings);
	} else {
			ESP_LOGE(MQTT_TAG, "Data %s not recognized", data_topic);
	}
	cJSON_Delete(root);

	ESP_LOGI(MQTT_TAG, "Settings updated");
	if(!get_is_settings_received()) settings_received();
}

static void initiate_ota(const char *mqtt_data) {
   const char *TAG = "INITIATE_OTA";

   char version[FIRMWARE_VERSION_LEN], endpoint[OTA_URL_SIZE];
   if (ESP_OK == parse_ota_parameters(mqtt_data, version, endpoint)) {
      if (ESP_OK == validate_ota_parameters(version, endpoint)) {
         ESP_LOGI(TAG, "FW upgrade command received over MQTT - checking for valid URL\n");
         if (strlen(endpoint) > OTA_URL_SIZE) {
            ESP_LOGI(TAG, "URL length is more than valid limit of: [%d]\r\n", OTA_URL_SIZE);
            publish_ota_result(mqtt_client, OTA_FAIL, INVALID_OTA_URL_RECEIVED);
         }
         else {
            /* Copy FW upgrade URL to local buffer */
            printf("Received URL lenght is: %d\r\n", strlen(endpoint));
            url_buf = (char *)malloc(strlen(endpoint) + 1);
            if (NULL == url_buf) {
               printf("Unable to allocate memory to save received URL\r\n");
               publish_ota_result(mqtt_client, OTA_FAIL, INVALID_OTA_URL_RECEIVED);
            }
            else {
               memset(url_buf, 0x00, strlen(endpoint) + 1);
               strncpy(url_buf, endpoint, strlen(endpoint));
               printf("Received URL is: %s\r\n", url_buf);

               /* Starting OTA thread */
               xTaskCreate(&ota_task, "ota_task", 8192, mqtt_client, 5, NULL);
            }
         }
      }
   }
}

static esp_err_t validate_ota_parameters(char *version, char *endpoint)
{
   const char *TAG = "VALIDATE_OTA_PARAMETERS";

   ESP_LOGI(TAG, "version: \"%s\"\n", version);
   ESP_LOGI(TAG, "endpoint: \"%s\"\n", endpoint);

   if (version == NULL || endpoint == NULL) {
      ESP_LOGI(TAG, "Invalid parameter received");
      return ESP_FAIL;
   }

   ESP_LOGI(TAG, "FW upgrade command received over MQTT - checking for valid URL\n");
   if (strlen(endpoint) > OTA_URL_SIZE) {
      ESP_LOGI(TAG, "URL length is more than valid limit of: [%d]\r\n", OTA_URL_SIZE);
      return ESP_FAIL;
   }

   return ESP_OK;
}

static esp_err_t parse_ota_parameters(const char *buffer, char *version_buf, char *endpoint_buf)
{
   const char *TAG = "PARSE_OTA_PARAMETERS";

   cJSON *version;
   cJSON *endpoint;

   if (buffer == NULL) {
      ESP_LOGI(TAG, "Invalid parameter received");
      return ESP_FAIL;
   }

   cJSON *root = cJSON_Parse(buffer);
   if (root == NULL) {
      ESP_LOGI(TAG, "Fail to deserialize Json");
      return ESP_FAIL;
   }

   version = cJSON_GetObjectItemCaseSensitive(root, "version");
   if (cJSON_IsString(version) && (version->valuestring != NULL)) {
      strcpy(version_buf, version->valuestring);
      ESP_LOGI(TAG, "version: \"%s\"\n", version_buf);
   }

   endpoint = cJSON_GetObjectItemCaseSensitive(root, "endpoint");
   if (cJSON_IsString(endpoint) && (endpoint->valuestring != NULL)) {
      strcpy(endpoint_buf, endpoint->valuestring);
      ESP_LOGI(TAG, "endpoint: \"%s\"\n", endpoint_buf);
   }
   return ESP_OK;
}

static void create_and_publish_ota_result(esp_mqtt_client_handle_t client, ota_result_t ota_result, ota_failure_reason_t ota_failure_reason)
{
   const char *TAG = "CREATE_AND_PUBLISH_OTA_RESULT";
   cJSON *root, *device_id, *version, *result, *error;

   // Initializing json object and sensor array
   root = cJSON_CreateObject();

   // Adding Device ID
   device_id = cJSON_CreateString(get_network_settings()->device_id);
   cJSON_AddItemToObject(root, "device_id", device_id);

   // Adding Device version
   esp_app_desc_t running_app_info;
   const esp_partition_t *running = esp_ota_get_running_partition();

   if (esp_ota_get_partition_description(running, &running_app_info) == ESP_OK) {
      ESP_LOGI(TAG, "Running firmware version: %s", running_app_info.version);
      version = cJSON_CreateString(running_app_info.version);
      cJSON_AddItemToObject(root, "version", version);
   }

   if (ota_result == OTA_SUCCESS) {
      result = cJSON_CreateString("success");
      cJSON_AddItemToObject(root, "result", result);
      error = cJSON_CreateString("");
      cJSON_AddItemToObject(root, "error", error);
   }
   else {
      result = cJSON_CreateString("failure");
      cJSON_AddItemToObject(root, "result", result);
      if (ota_failure_reason == VERSION_NOT_FOUND) {
         error = cJSON_CreateString("version not found");
      }
      else if (ota_failure_reason == INVALID_OTA_URL_RECEIVED) {
         error = cJSON_CreateString("Invalid OTA URL received");
      }
      else if (ota_failure_reason == HTTP_CONNECTION_FAILED) {
         error = cJSON_CreateString("http connection failed");
      }
      else if (ota_failure_reason == OTA_UPDATE_FAILED) {
         error = cJSON_CreateString("ota update failed");
      }
      else if (ota_failure_reason == IMAGE_FILE_LARGER_THAN_OTA_PARTITION) {
         error = cJSON_CreateString("image file larger than ota partition");
      }
      else if (ota_failure_reason == OTA_WRTIE_OPERATION_FAILED) {
         error = cJSON_CreateString("ota write operation failed");
      }
      else if (ota_failure_reason == IMAGE_VALIDATION_FAILED) {
         error = cJSON_CreateString("version not found");
      }
      else if (ota_failure_reason == OTA_SET_BOOT_PARTITION_FAILED) {
         error = cJSON_CreateString("version not found");
      }
      else{
         error = cJSON_CreateString("version not found");
      }

      cJSON_AddItemToObject(root, "error", error);
   }

   // Creating string from JSON
   char *data = cJSON_PrintUnformatted(root);

   // Free memory
   cJSON_Delete(root);

   ESP_LOGI(TAG, "Message: %s", data);

   esp_mqtt_client_publish(client, ota_done_topic, data, 0, 1, 0);

   ESP_LOGI(TAG, "ota_failed message publish successful, Message: %s", data);
}

void publish_ota_result(esp_mqtt_client_handle_t client, ota_result_t ota_result, ota_failure_reason_t ota_failure_reason) {
   create_and_publish_ota_result(client, ota_result, ota_failure_reason);
}

void data_handler(char *topic_in, uint32_t topic_len, char *data_in, uint32_t data_len) {
	const char *TAG = "DATA_HANDLER";

	// Create dynamically allocated vars for topic and data
	char *topic = malloc(sizeof(char) * (topic_len+1));
	char *data = malloc(sizeof(char) * (data_len+1));

	// Copy over topic
	for(int i = 0; i < topic_len; i++) {
		topic[i] = topic_in[i];
	}
	topic[topic_len] = 0;

	// Copy over data
	for(int i = 0; i < data_len; i++) {
		data[i] = data_in[i];
	}
	data[data_len] = 0;

	ESP_LOGI(TAG, "Incoming Topic: %s", topic);

	// Check topic against each subscribed topic possible
	if(strcmp(topic, sensor_settings_topic) == 0) {
		ESP_LOGI(TAG, "Sensor settings received");

		// Update sensor settings
		update_settings(data);
	} else if(strcmp(topic, grow_cycle_topic) == 0) {
		ESP_LOGI(TAG, "Grow cycle status received");

		// Start/stop grow cycle according to message
		if(data[0] == '0') stop_grow_cycle();
		else start_grow_cycle();
	} else if(strcmp(topic, rf_control_topic) == 0) {
		cJSON *obj = cJSON_Parse(data);
		obj = obj->child;
		ESP_LOGI(TAG, "RF id number %d: RF state: %d", atoi(obj->string), obj->valueint);
		control_power_outlet(atoi(obj->string), obj->valueint);
	} else if(strcmp(topic, ota_update_topic) == 0) {
      // Initiate ota
      ESP_LOGI(TAG, "OTA update message received");
      initiate_ota(data);
   } else if(strcmp(topic, version_request_topic) == 0) {
      // Send back firmware version
      ESP_LOGI(TAG, "Firmware version requested");
      publish_firmware_version();
   } else {
		// Topic doesn't match any known topics
		ESP_LOGE(TAG, "Topic unknown");
	}

	free(topic);
	free(data);
}

static void publish_firmware_version() {
   cJSON *root, *device_id, *version;
   root = cJSON_CreateObject();

   // Adding Device ID
   device_id = cJSON_CreateString(get_network_settings()->device_id);
   cJSON_AddItemToObject(root, "device_id", device_id);

   // Adding version
   char firmware_version[FIRMWARE_VERSION_LEN];
   if(get_firmware_version(firmware_version)) {
      version = cJSON_CreateString(firmware_version);
   } else {
      version = cJSON_CreateString("error");
   }
   cJSON_AddItemToObject(root, "version", version);

   esp_mqtt_client_publish(mqtt_client, version_result_topic, cJSON_PrintUnformatted(root), 0, 1, 0);
   cJSON_Delete(root);
}