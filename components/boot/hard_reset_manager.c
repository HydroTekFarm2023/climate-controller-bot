#include "hard_reset_manager.h"

#include <time.h>
#include <sys/time.h>
#include <esp_log.h>
#include <esp_sleep.h>
#include <esp32/ulp.h>
#include <driver/gpio.h>
#include <driver/touch_pad.h>
#include <driver/adc.h>
#include <driver/rtc_io.h>
#include <soc/sens_periph.h>
#include <soc/rtc.h>
#include <driver/timer.h>
#include <driver/periph_ctrl.h>
#include "nvs_manager.h"

#include "ports.h"

unsigned long get_current_time() {
    /* Get time */
    if (gettimeofday(&current_time, NULL) == 0) {
        return current_time.tv_sec;
    }
    return 0UL; 
}

static void IRAM_ATTR reset_button_isr_handler (void* arg) {
   BaseType_t xHigherPriorityTaskWoken;

   xHigherPriorityTaskWoken = false;
   xSemaphoreGiveFromISR(xBinarySemph, &xHigherPriorityTaskWoken);

   portYIELD_FROM_ISR();
}

esp_err_t init_reset_semaphore() {
    xBinarySemph = xSemaphoreCreateBinary();
    if (xBinarySemph != NULL) {
        return ESP_OK;
    } else {
        printf("Could not allocate memory for semaphore\n");
        return ESP_FAIL;
    }
}

void hard_reset(void *args) {
    for (;;) {
        xSemaphoreTake(xBinarySemph, portMAX_DELAY);
        int task_priority = uxTaskPriorityGet(NULL);
        vTaskPrioritySet(NULL, (configMAX_PRIORITIES - 1)); // Temporarily increase priority so that hard reset can take place without interruption
         //Make sure we get start time // 
        unsigned long start = get_current_time();
        //Keep checking if button is pressed at lest 10 seconds then perform reset tasks//
        while (gpio_get_level(HARD_RESET_GPIO) == 0) {
            unsigned long curr_time = get_current_time();
            if ((curr_time - start >= 10)) {
                ESP_LOGI(HARD_RESET_TAG, "Hard Rest Initiated.");
                nvs_clear();
                restart_esp_32();
                //esp_restart will not return back // 
                break; 
        }
     }
     //If reset did not occur make sure to reset task priority back//
     vTaskPrioritySet(NULL, task_priority);

    }
}

void init_hard_reset_button() {
    // Create Falling Edge Interrupt on Hard Reset Button GPIO
    gpio_config_t gpio_conf;
    gpio_conf.intr_type = GPIO_INTR_NEGEDGE;
    gpio_conf.pin_bit_mask = GPIO_INPUT_PIN_SEL_RESET;
    gpio_conf.mode = GPIO_MODE_INPUT;
    gpio_conf.pull_up_en = 1;
    gpio_config(&gpio_conf);

    // ISR handler for specific GPIO pin
    gpio_isr_handler_add(HARD_RESET_GPIO, reset_button_isr_handler, (void*) HARD_RESET_GPIO);
}

void restart_esp_32() { // Restart ESP32
	fflush(stdout);
	esp_restart();
}



