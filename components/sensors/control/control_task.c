#include "control_task.h"

#include <esp_log.h>
#include <sdkconfig.h>

#include "sensor_control.h"
#include "co2_control.h"
#include "humidity_control.h"
#include "temperature_control.h"
#include "control_settings_keys.h"
#include "sync_sensors.h"
#include "ports.h"
#include "mqtt_manager.h"
#include "rf_transmitter.h"

void init_control() {

	//For Co2 //
	init_sensor_control(get_co2_control(), "CO2_CONTROL", get_co2_control_status(), CO2_MARGIN_ERROR);
	is_co2_control_on = false;

	//Humidity// 
	init_sensor_control(get_humidity_control(), "HUMIDITY_CONTROL", get_humidity_control_status(), HUMIDITY_MARGIN_ERROR);
	is_humidity_control_on = false;

	//Temperature// 
	init_sensor_control(get_temperature_control(), "TEMPERATURE_CONTROL", get_temperature_control_status(), TEMPERATURE_MARGIN_ERROR);
	is_temperature_control_on = false;

}

void sensor_control (void *parameter) {
	for(;;)  {
		// Check sensors
		check_co2();
		check_temperature();
		check_humidity();

		// Wait till next sensor readings
		vTaskDelay(pdMS_TO_TICKS(SENSOR_MEASUREMENT_PERIOD));
	}
}
