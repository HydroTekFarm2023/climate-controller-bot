idf_component_register(
	SRCS "network_settings.c" "access_point/access_point.c" "mqtt/mqtt_manager.c" "wifi/wifi_connect.c"
	INCLUDE_DIRS "." "access_point/" "mqtt/" "wifi/" 	
	PRIV_REQUIRES boot sensors rtc json nvs_manager log grow_manager nvs_flash
	REQUIRES esp_http_server mqtt
)

