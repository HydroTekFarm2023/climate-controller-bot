#include "ota.h"

char *url_buf = {0};
bool  is_ota_success_on_bootup = false;

/*an ota data write buffer ready to write to the flash*/
static char ota_write_data[BUFFSIZE + 1] = { 0 };

void http_cleanup(esp_http_client_handle_t client)
{
   esp_http_client_close(client);
   esp_http_client_cleanup(client);
}

void __attribute__((noreturn)) task_fatal_error()
{
   const char *TAG = "OTA_TASK_FATAL_ERR";
   ESP_LOGE(TAG, "Exiting task due to fatal error...");
   (void)vTaskDelete(NULL);

   while (1) {
      ;
   }
}

void print_sha256 (const uint8_t *image_hash, const char *label)
{
   const char *TAG = "OTA_PRINT_sha256";
   char hash_print[HASH_LEN * 2 + 1];
   hash_print[HASH_LEN * 2] = 0;
   for (int i = 0; i < HASH_LEN; ++i) {
      sprintf(&hash_print[i * 2], "%02x", image_hash[i]);
   }
   ESP_LOGI(TAG, "%s: %s", label, hash_print);
}

void ota_task(void *pvParameter)
{
   const char *TAG = "OTA_TASK";
   esp_err_t err;
   /* update handle : set by esp_ota_begin(), must be freed via esp_ota_end() */
   esp_ota_handle_t update_handle = 0 ;
   const esp_partition_t *update_partition = NULL;
   esp_mqtt_client_handle_t client_mqtt = (esp_mqtt_client_handle_t) pvParameter;

   ESP_LOGI(TAG, "Starting OTA");

   /* Ensure to disable any WiFi power save mode, this allows best throughput
    * and hence timings for overall OTA operation.
    */
   esp_wifi_set_ps(WIFI_PS_NONE);

   const esp_partition_t *configured = esp_ota_get_boot_partition();
   const esp_partition_t *running = esp_ota_get_running_partition();

   if (configured != running) {
      ESP_LOGW(TAG, "Configured OTA boot partition at offset 0x%08x, but running from offset 0x%08x",
            configured->address, running->address);
      ESP_LOGW(TAG, "(This can happen if either the OTA boot data or preferred boot image become corrupted somehow.)");
   }
   ESP_LOGI(TAG, "Running partition type %d subtype %d (offset 0x%08x)",
         running->type, running->subtype, running->address);

   esp_http_client_config_t config = {
      .url = url_buf,
      .timeout_ms = OTA_RECV_TIMEOUT,
   };

   config.skip_cert_common_name_check = true;

   esp_http_client_handle_t client = esp_http_client_init(&config);
   if (client == NULL) {
      ESP_LOGE(TAG, "Failed to initialise HTTP connection");
      publish_ota_result(client_mqtt, OTA_FAIL, HTTP_CONNECTION_FAILED);
      task_fatal_error();
   }
   err = esp_http_client_open(client, 0);
   if (err != ESP_OK) {
      ESP_LOGE(TAG, "Failed to open HTTP connection: %s", esp_err_to_name(err));
      esp_http_client_cleanup(client);
      publish_ota_result(client_mqtt, OTA_FAIL, HTTP_CONNECTION_FAILED);
      task_fatal_error();
   }
   esp_http_client_fetch_headers(client);

   update_partition = esp_ota_get_next_update_partition(NULL);
   ESP_LOGI(TAG, "Writing to partition subtype %d at offset 0x%x",
         update_partition->subtype, update_partition->address);
   assert(update_partition != NULL);

   int binary_file_length = 0;
   /*deal with all receive packet*/
   bool image_header_was_checked = false;
   while (1) {
      int data_read = esp_http_client_read(client, ota_write_data, BUFFSIZE);
      if (data_read < 0) {
         ESP_LOGE(TAG, "Error: SSL data read error");
         http_cleanup(client);
         publish_ota_result(client_mqtt, OTA_FAIL, OTA_UPDATE_FAILED);
         task_fatal_error();
      } else if (data_read > 0) {
         if (image_header_was_checked == false) {
            esp_app_desc_t new_app_info;
            if (data_read > sizeof(esp_image_header_t) + sizeof(esp_image_segment_header_t) + sizeof(esp_app_desc_t)) {
               // check current version with downloading
               memcpy(&new_app_info, &ota_write_data[sizeof(esp_image_header_t) + sizeof(esp_image_segment_header_t)], sizeof(esp_app_desc_t));
               ESP_LOGI(TAG, "New firmware version: %s", new_app_info.version);

               esp_app_desc_t running_app_info;
               if (esp_ota_get_partition_description(running, &running_app_info) == ESP_OK) {
                  ESP_LOGI(TAG, "Running firmware version: %s", running_app_info.version);
               }

               const esp_partition_t* last_invalid_app = esp_ota_get_last_invalid_partition();
               esp_app_desc_t invalid_app_info;
               if (esp_ota_get_partition_description(last_invalid_app, &invalid_app_info) == ESP_OK) {
                  ESP_LOGI(TAG, "Last invalid firmware version: %s", invalid_app_info.version);
               }

               // check current version with last invalid partition
               if (last_invalid_app != NULL) {
                  if (memcmp(invalid_app_info.version, new_app_info.version, sizeof(new_app_info.version)) == 0) {
                     ESP_LOGW(TAG, "New version is the same as invalid version.");
                     ESP_LOGW(TAG, "Previously, there was an attempt to launch the firmware with %s version, but it failed.", invalid_app_info.version);
                     ESP_LOGW(TAG, "The firmware has been rolled back to the previous version.");
                     http_cleanup(client);
                     task_fatal_error();
                  }
               }
               if (memcmp(new_app_info.version, running_app_info.version, sizeof(new_app_info.version)) == 0) {
                  ESP_LOGW(TAG, "Current running version is the same as a new. We will not continue the update.");
                  http_cleanup(client);
                  task_fatal_error();
               }

               image_header_was_checked = true;

               err = esp_ota_begin(update_partition, OTA_SIZE_UNKNOWN, &update_handle);
               if (err != ESP_OK) {
                  ESP_LOGE(TAG, "esp_ota_begin failed (%s)", esp_err_to_name(err));
                  http_cleanup(client);
                  publish_ota_result(client_mqtt, OTA_FAIL, OTA_UPDATE_FAILED);
                  task_fatal_error();
               }
               ESP_LOGI(TAG, "esp_ota_begin succeeded");
            } else {
               ESP_LOGE(TAG, "received package is not fit len");
               http_cleanup(client);
               publish_ota_result(client_mqtt, OTA_FAIL, IMAGE_FILE_LARGER_THAN_OTA_PARTITION);
               task_fatal_error();
            }
         }
         err = esp_ota_write( update_handle, (const void *)ota_write_data, data_read);
         if (err != ESP_OK) {
            http_cleanup(client);
            publish_ota_result(client_mqtt, OTA_FAIL, OTA_WRTIE_OPERATION_FAILED);
            task_fatal_error();
         }
         binary_file_length += data_read;
         ESP_LOGD(TAG, "Written image length %d", binary_file_length);
      } else if (data_read == 0) {
         /*
          * As esp_http_client_read never returns negative error code, we rely on
          * `errno` to check for underlying transport connectivity closure if any
          */
         if (errno == ECONNRESET || errno == ENOTCONN) {
            ESP_LOGE(TAG, "Connection closed, errno = %d", errno);
            break;
         }
         if (esp_http_client_is_complete_data_received(client) == true) {
            ESP_LOGI(TAG, "Connection closed");
            break;
         }
      }
   }
   ESP_LOGI(TAG, "Total Write binary data length: %d", binary_file_length);
   if (esp_http_client_is_complete_data_received(client) != true) {
      ESP_LOGE(TAG, "Error in receiving complete file");
      http_cleanup(client);
      publish_ota_result(client_mqtt, OTA_FAIL, OTA_WRTIE_OPERATION_FAILED);
      task_fatal_error();
   }

   err = esp_ota_end(update_handle);
   if (err != ESP_OK) {
      if (err == ESP_ERR_OTA_VALIDATE_FAILED) {
         ESP_LOGE(TAG, "Image validation failed, image is corrupted");
      }
      ESP_LOGE(TAG, "esp_ota_end failed (%s)!", esp_err_to_name(err));
      http_cleanup(client);
      publish_ota_result(client_mqtt, OTA_FAIL, IMAGE_VALIDATION_FAILED);
      task_fatal_error();
   }

   err = esp_ota_set_boot_partition(update_partition);
   if (err != ESP_OK) {
      ESP_LOGE(TAG, "esp_ota_set_boot_partition failed (%s)!", esp_err_to_name(err));
      http_cleanup(client);
      publish_ota_result(client_mqtt, OTA_FAIL, OTA_SET_BOOT_PARTITION_FAILED);
      task_fatal_error();
   }
   ESP_LOGI(TAG, "Prepare to restart system!");

   /* Publish OTA successful message over MQTT */
   vTaskDelay(2000 / portTICK_PERIOD_MS);

   /* Restart ESP with latest firmware */
   esp_restart();
   return ;
}

bool diagnostic(void)
{
   const char *TAG = "OTA_DIAG";
   gpio_config_t io_conf;
   io_conf.intr_type    = GPIO_PIN_INTR_DISABLE;
   io_conf.mode         = GPIO_MODE_INPUT;
   io_conf.pin_bit_mask = (1ULL << GPIO_DIAGNOSTIC);
   io_conf.pull_down_en = GPIO_PULLDOWN_DISABLE;
   io_conf.pull_up_en   = GPIO_PULLUP_ENABLE;
   gpio_config(&io_conf);

   ESP_LOGI(TAG, "Diagnostics (5 sec)...");
   vTaskDelay(5000 / portTICK_PERIOD_MS);

   bool diagnostic_is_ok = gpio_get_level(GPIO_DIAGNOSTIC);

   gpio_reset_pin(GPIO_DIAGNOSTIC);
   return diagnostic_is_ok;
}

void init_ota()
{
   const char *TAG = "OTA_INIT";
   uint8_t sha_256[HASH_LEN] = { 0 };
   esp_partition_t partition;

   ESP_LOGI(TAG, "[APP] Startup..");
   ESP_LOGI(TAG, "[APP] Free memory: %d bytes", esp_get_free_heap_size());
   ESP_LOGI(TAG, "[APP] IDF version: %s", esp_get_idf_version());

   esp_log_level_set("*", ESP_LOG_INFO);
   esp_log_level_set("MQTT_CLIENT", ESP_LOG_VERBOSE);
   esp_log_level_set("MQTT_EXAMPLE", ESP_LOG_VERBOSE);
   esp_log_level_set("TRANSPORT_TCP", ESP_LOG_VERBOSE);
   esp_log_level_set("TRANSPORT_SSL", ESP_LOG_VERBOSE);
   esp_log_level_set("TRANSPORT", ESP_LOG_VERBOSE);
   esp_log_level_set("OUTBOX", ESP_LOG_VERBOSE);

   // get sha256 digest for the partition table
   partition.address   = ESP_PARTITION_TABLE_OFFSET;
   partition.size      = ESP_PARTITION_TABLE_MAX_LEN;
   partition.type      = ESP_PARTITION_TYPE_DATA;
   esp_partition_get_sha256(&partition, sha_256);
   print_sha256(sha_256, "SHA-256 for the partition table: ");

   // get sha256 digest for bootloader
   partition.address   = ESP_BOOTLOADER_OFFSET;
   partition.size      = ESP_PARTITION_TABLE_OFFSET;
   partition.type      = ESP_PARTITION_TYPE_APP;
   esp_partition_get_sha256(&partition, sha_256);
   print_sha256(sha_256, "SHA-256 for bootloader: ");

   // get sha256 digest for running partition
   esp_partition_get_sha256(esp_ota_get_running_partition(), sha_256);
   print_sha256(sha_256, "SHA-256 for current firmware: ");

   const esp_partition_t *running = esp_ota_get_running_partition();
   esp_ota_img_states_t ota_state;
   if (esp_ota_get_state_partition(running, &ota_state) == ESP_OK) {
      if (ota_state == ESP_OTA_IMG_PENDING_VERIFY) {
         // run diagnostic function ...
         // bool diagnostic_is_ok = diagnostic(); TODO Talk to Neel and Vidit and add back in diagnostics at some point
            bool diagnostic_is_ok = true; // TODO Remove this line when diagnostics is added back in
         if (diagnostic_is_ok) {
            ESP_LOGI(TAG, "Diagnostics completed successfully! Continuing execution ...");
            is_ota_success_on_bootup = true;
            esp_ota_mark_app_valid_cancel_rollback();
         } else {
            ESP_LOGE(TAG, "Diagnostics failed! Start rollback to the previous version ...");
            is_ota_success_on_bootup = false;
            esp_ota_mark_app_invalid_rollback_and_reboot();
         }
      }
   }

   char version[FIRMWARE_VERSION_LEN];
   if(get_firmware_version(version)) {
      ESP_LOGI(TAG, "Firmware version: %s", version);
   } else {
      ESP_LOGI(TAG, "Could not get firmware version");
   }
}

bool get_firmware_version(char *version) {
   const esp_partition_t *running = esp_ota_get_running_partition();
   esp_app_desc_t running_app_info;
   if (esp_ota_get_partition_description(running, &running_app_info) == ESP_OK) {
      strcpy(version, running_app_info.version);
      return true;
   } else {
      return false;
   }
}
