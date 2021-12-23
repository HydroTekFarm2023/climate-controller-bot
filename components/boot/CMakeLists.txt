idf_component_register(
	SRCS "boot.c" "deep_sleep_manager.c"  "hard_reset_manager.c" "led_manager.c"
	INCLUDE_DIRS "." 	
	REQUIRES esp_adc_cal sensors ulp
	PRIV_REQUIRES nvs_flash sensors rtc network_manager rf_transmitter nvs_manager grow_manager
)