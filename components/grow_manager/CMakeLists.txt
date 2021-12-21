idf_component_register(
	SRCS "grow_manager.c"
	INCLUDE_DIRS "."	
	PRIV_REQUIRES nvs_manager freertos sensors network_manager rf_transmitter rtc nvs_flash
	REQUIRES 
)