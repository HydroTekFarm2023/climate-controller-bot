#include <Freertos/freertos.h>
#include <Freertos/task.h>

// Task handle
TaskHandle_t sensor_control_task_handle;

// Init control
void init_control();

// Sensor control task
void sensor_control();

// Reset sensor checks array
void reset_sensor_checks();


