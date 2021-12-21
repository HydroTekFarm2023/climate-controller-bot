#include <Freertos/freertos.h>
#include <Freertos/task.h>
#include <Freertos/semphr.h>
#define ESP_INTR_FLAG_DEFAULT 0
#define GPIO_INPUT_PIN_SEL_RESET (1ULL << HARD_RESET_GPIO);

#define HARD_RESET_TAG "HARD_RESET_MANAGER"
//Task Handle for hard reset when interrupt occurs// 
TaskHandle_t hard_reset_task_handle;
//Semaphore to indicate when to defer work to hard reset task // 
SemaphoreHandle_t xBinarySemph;
//struct to keep track of time interval of pressed button// 
struct timeval current_time; 
//function to create semaphore// 
esp_err_t init_reset_semaphore();
//deferred task to complete hard reset//
void hard_reset(void *args);
//init hard reset GPIO// 
void init_hard_reset_button();
//function to restart esp32
void restart_esp_32();