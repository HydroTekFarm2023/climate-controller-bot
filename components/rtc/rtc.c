#include "rtc.h"
#include <string.h>
#include <esp_log.h>
#include <esp_sntp.h>
#include <freertos/FreeRTOS.h>

#include "rf_transmitter.h"
#include "task_priorities.h"
#include "nvs_namespace_keys.h"
#include "ports.h"
#include "sensor_control.h"

void get_date_time(struct tm *time) {
	// Get current time and set it to return var
	ds3231_get_time(&dev, time);
}

void init_rtc() { // Init RTC
	memset(&dev, 0, sizeof(i2c_dev_t));
	ESP_ERROR_CHECK(ds3231_init_desc(&dev, 0, SDA_GPIO, SCL_GPIO));
	set_time();

}

void init_sntp() {
	sntp_setoperatingmode(SNTP_OPMODE_POLL);
    sntp_setservername(0, "pool.ntp.org");
	sntp_init();

	while (sntp_get_sync_status() == SNTP_SYNC_STATUS_RESET) {
        ESP_LOGI("", "Waiting for system time to be set...");
        vTaskDelay(pdMS_TO_TICKS(2000));
    }
}

void set_time() { // Set current time to some date
	time_t now;
	struct tm dateTime;
	time(&now);
	localtime_r(&now, &dateTime);
	ESP_LOGI("", "Current time: %li", now);
	ESP_ERROR_CHECK(ds3231_set_time(&dev, &dateTime));

}


void manage_timers_alarms(void *parameter) {
	const char *TAG = "TIMER_TASK";

	for(;;) {
		// Get current unix time
		time_t unix_time;
		get_unix_time(&dev, &unix_time);
		vTaskDelay(TIMER_ALARM_REGULAR_DELAY);
	}
}



