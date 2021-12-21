#include "ds3231.h"

#include <cJSON.h>

// RTC dev
i2c_dev_t dev;

// Timer and alarm periods
#define TIMER_ALARM_URGENT_DELAY 10
#define TIMER_ALARM_REGULAR_DELAY 50

// Keys

// Task handle
TaskHandle_t timer_alarm_task_handle;

// Timers -> add timers below


// Initialize rtc
void init_rtc();

// Initialize sntp server
void init_sntp();

// Set current time
void set_time();

// Get current day and time
void get_date_time(struct tm *time);

// Timer and alarm task
void manage_timers_alarms();


