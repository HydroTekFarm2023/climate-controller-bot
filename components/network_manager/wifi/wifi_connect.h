#include <stdbool.h>
#include <freertos/FreeRTOS.h>
#include <freertos/event_groups.h>

// WiFi bits
#define WIFI_CONNECTED_BIT (1<<0)
#define WIFI_FAIL_BIT      (1<<1)

#define RETRYMAX 5 // WiFi MAX Reconnection Attempts

int retryNumber;  // WiFi Reconnection Attempts

bool is_wifi_connected; // Is wifi connected

// WiFi Coordination with Event Group
EventGroupHandle_t wifi_event_group;

// Connect ESP32 to wifi
bool connect_wifi();

//Getter for is_wifi_connected//
bool get_is_wifi_connected();

