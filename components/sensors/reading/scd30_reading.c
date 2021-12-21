#include "scd30_reading.h"
#include <esp_log.h>
#include <string.h>
#include "scd30.h"
#include "sync_sensors.h"
#include "task_priorities.h"
#include "ports.h"

struct sensor* get_temperature_sensor() { return &temperature_sensor; }

struct sensor* get_humidity_sensor() { return &humidity_sensor;}

struct sensor* get_co2_sensor() { return &co2_sensor;}

void measure_scd30(void *parameter) {     // SCD30 Sensor Measurement Task
    const char *TAG = "SCD30_Task";

    init_sensor(&temperature_sensor, "temperature", true, false);

    init_sensor(&humidity_sensor, "humidity", true, false);

    init_sensor(&co2_sensor, "humidity", true, false);

    scd30_sensor_t dev;
	memset(&dev, 0, sizeof(scd30_sensor_t));
	ESP_ERROR_CHECK(scd30_init(&dev, 0, SCD30_I2C_ADDR, SDA_GPIO, SCL_GPIO)); // Initialize SCD30 I2C communication
    vTaskDelay(pdMS_TO_TICKS(1000));

    ESP_ERROR_CHECK(scd30_trigger_continuous_measurement(&dev, 0)); 

    ESP_ERROR_CHECK(scd30_set_measurement_interval(&dev, 5)); 
    for(;;) {
        // trigger the sensor to start one TPHG measurement cycle
        bool check = false; 
        vTaskDelay(pdMS_TO_TICKS(1000));
        if (scd30_get_data_ready_status(&dev, &check) == ESP_OK) {
            // get the results and do something with them
            if (check) {
                float temp = 0.0f, co2 = 0.0f, humidity = 0.0f; 
                scd30_read_measurement(&dev, &co2, &temp, &humidity);
                ESP_LOGI(TAG, "CO2: %.2f", co2);
                ESP_LOGI(TAG, "Temperature: %.2f", temp);
                ESP_LOGI(TAG, "Humidity: %.2f\n", humidity);

                float *co2_val = sensor_get_address_value(&co2_sensor);
                *co2_val = temp;

                float *temp_val = sensor_get_address_value(&temperature_sensor);
                *temp_val = temp;
                
                 float *humidity_val = sensor_get_address_value(&humidity_sensor);
                *humidity_val = humidity; 

            } else {
                //ESP_LOGI(TAG, "Data not ready yet");
            }
        } else {
               // ESP_LOGE(TAG, "Not OK!!");
        }
        // Set SCD BIT
        xEventGroupSetBits(sensor_event_group, SCD_BIT);
    }

}
