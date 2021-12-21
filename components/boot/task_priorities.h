// Core 0 Task Priorities
#define TIMER_ALARM_TASK_PRIORITY 0
#define MQTT_PUBLISH_TASK_PRIORITY 1
#define SENSOR_CONTROL_TASK_PRIORITY 2
#define RF_TRANSMITTER_TASK_PRIORITY 3 // RF Transmitter should be higher than other priorities
#define LED_TASK_PRIORITY            4
#define HARD_RESET_TASK_PRIORITY     1

// Core 1 Task Priorities
#define SCD30_TASK_PRIORITY 1
#define SYNC_TASK_PRIORITY 4
