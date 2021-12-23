#include "deep_sleep_manager.h"

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

void IRAM_ATTR timer_group0_isr(void *param) {
   // configure GPIO for wakeup device from sleep mode
   esp_sleep_enable_ext0_wakeup(POWER_BUTTON_GPIO, 0);
   // start sleep mode
   esp_deep_sleep_start();
}


static void IRAM_ATTR power_button_isr_handler (void* arg) {
   // reset last time;
   timer_pause(TIMER_GROUP_0, TIMER_0);
   // reset timer counter
   timer_set_counter_value(TIMER_GROUP_0, TIMER_0, 0x00000000ULL);
   // restart/start timer
   timer_start(TIMER_GROUP_0, TIMER_0);
}

static void tg0_timer_init(int timer_idx, double timer_interval_sec)
{
    /* Select and initialize basic parameters of the timer */
    timer_config_t config = {
        .divider = TIMER_DIVIDER,
        .counter_dir = TIMER_COUNT_UP,
        .counter_en = TIMER_PAUSE,
        .alarm_en = TIMER_ALARM_EN,
        .auto_reload = 0,
    }; // default clock source is APB
    timer_init(TIMER_GROUP_0, timer_idx, &config);

    // Timer's counter will initially start from value below.
    timer_set_counter_value(TIMER_GROUP_0, timer_idx, 0x00000000ULL);

    // Configure the alarm value and the interrupt on alarm.
    timer_set_alarm_value(TIMER_GROUP_0, timer_idx, timer_interval_sec * TIMER_SCALE);
    timer_enable_intr(TIMER_GROUP_0, timer_idx);
    timer_isr_register(TIMER_GROUP_0, timer_idx, timer_group0_isr,
          (void *) timer_idx, ESP_INTR_FLAG_IRAM, NULL);
}

void init_power_button() {
	// Create Falling Edge Interrupt on Power Button GPIO
	gpio_config_t gpio_conf;
	gpio_conf.intr_type = GPIO_INTR_NEGEDGE;
	gpio_conf.pin_bit_mask = GPIO_INPUT_PIN_SEL;
	gpio_conf.mode = GPIO_MODE_INPUT;
	gpio_conf.pull_up_en = 1;
	gpio_config(&gpio_conf);

	// Install GPIO ISR Service
	gpio_install_isr_service(ESP_INTR_FLAG_DEFAULT);

	// ISR handler for specific GPIO pin
	gpio_isr_handler_add(POWER_BUTTON_GPIO, power_button_isr_handler, (void*) POWER_BUTTON_GPIO);

	tg0_timer_init(TIMER_0, BUTTON_PRESS_INTERVAL);

	/* Detect whether boot is caused because of deep sleep reset or not */
	if((esp_sleep_get_wakeup_cause()) == ESP_SLEEP_WAKEUP_EXT0)
	{
		ESP_LOGI(DEEP_SLEEP_TAG, "Wake up from deep Sleep");
	}
	else
	{
		ESP_LOGI(DEEP_SLEEP_TAG, "Not a deep sleep reset");
	}
}
