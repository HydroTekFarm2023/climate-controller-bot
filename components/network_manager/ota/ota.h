#ifndef __OTA_H
#define __OTA_H

#include "mqtt_manager.h"
#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "esp_log.h"
#include "esp_ota_ops.h"
#include "esp_http_client.h"
#include "esp_flash_partitions.h"
#include "esp_partition.h"
#include "nvs.h"
#include "errno.h"
#include "driver/gpio.h"

#include "esp_wifi.h"
#include "tcpip_adapter.h"
#include "freertos/semphr.h"
#include "freertos/queue.h"

#include "lwip/sockets.h"
#include "lwip/dns.h"
#include "lwip/netdb.h"
#include "mqtt_client.h"

#define BUFFSIZE                1024
#define HASH_LEN                32 /* SHA-256 digest length */
#define OTA_RECV_TIMEOUT        5000
#define OTA_URL_SIZE            256
#define GPIO_DIAGNOSTIC         4
#define FIRMWARE_VERSION_LEN    16

void http_cleanup(esp_http_client_handle_t client);
void __attribute__((noreturn)) task_fatal_error();
void print_sha256 (const uint8_t *image_hash, const char *label);
void infinite_loop(void);
void ota_task(void *pvParameter);
bool diagnostic(void);
void init_ota();
bool get_firmware_version(char *version);

#endif//__OTA_H
