#include <freertos/FreeRTOS.h>
#include <driver/gpio.h>
#include <Freertos/task.h>

#define WIFI_LED_TAG "WIFI_LED_MANAGER"
//Task Handle for led// 
TaskHandle_t led_task_handle;
//Led manager task for green (wifi) led // 
void wifi_led(void *args);
