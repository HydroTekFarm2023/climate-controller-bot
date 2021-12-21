#include "led_manager.h"
#include "ports.h"
#include "wifi_connect.h"
#include <stdbool.h>
#include <esp_log.h>

void wifi_led(void *args) {
    //Turn on Green led when esp32 boots up
	gpio_set_direction(GREEN_LED, GPIO_MODE_OUTPUT);
	gpio_set_level(GREEN_LED, 1);
    gpio_set_direction(BLUE_LED, GPIO_MODE_OUTPUT);
    printf("Done\n");
    gpio_set_level(BLUE_LED, 1);
    for (;;) {
        if (get_is_wifi_connected()) {
            gpio_set_level(BLUE_LED, 1);
            vTaskDelay(pdMS_TO_TICKS(5000));
        } else {
            //If not Connected then blink blue led//
            gpio_set_level(BLUE_LED, 1);
            vTaskDelay(pdMS_TO_TICKS(1000));
            gpio_set_level(BLUE_LED, 0);
            vTaskDelay(pdMS_TO_TICKS(1000));
        }
    }
}