idf_component_register(
	SRCS "rtc.c"
	INCLUDE_DIRS "." 	
	REQUIRES sensors
	PRIV_REQUIRES boot
)