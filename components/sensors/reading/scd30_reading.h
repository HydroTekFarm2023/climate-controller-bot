#include <stdbool.h>
#include <Freertos/freertos.h>
#include <Freertos/task.h>
#include "sensor.h"

struct sensor temperature_sensor;

struct sensor humidity_sensor; 

struct sensor co2_sensor; 

// Get temperature sensor
struct sensor* get_temperature_sensor();

//Get humidity sensor
struct sensor * get_humidity_sensor();

//Get co2 sensor
struct sensor * get_co2_sensor();

// Measures scd sensor (co2, temperature, and humidity)
void measure_scd30();