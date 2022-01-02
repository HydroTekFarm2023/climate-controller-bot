idf_component_register(
	SRCS "rtc.c" "ds3231.c"
	INCLUDE_DIRS "." 	
	REQUIRES sensors
	PRIV_REQUIRES boot
)