idf_component_register(
	SRCS 
	"control/control_task.c"
	"control/co2_control.c"
	"control/humidity_control.c"
	"control/temperature_control.c"  
	"control/sensor_control.c"
	"libs/scd30.c"
	"libs/i2cdev.c" 
	"reading/scd30_reading.c"
	"reading/sensor.c"
	"reading/sync_sensors.c"
	INCLUDE_DIRS "control/" "libs/" "reading/" 	
	REQUIRES boot rtc rf_transmitter nvs_flash json log nvs_manager nvs_flash network_manager
	PRIV_REQUIRES 
)