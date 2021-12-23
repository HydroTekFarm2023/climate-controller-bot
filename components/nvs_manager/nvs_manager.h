#include <stdbool.h>
#include <stdio.h>
#include <nvs.h>

#define NVS_TAG "NVS_MANAGER"


// ----------------------------------------------------- MEMBER FUNCTIONS -----------------------------------------------------------------------

// Initializes nvs for use
void init_nvs();

// Clear nvs data
void nvs_clear();

// Get NVS handle
nvs_handle_t* nvs_get_handle(char *namespace);

// NVS setters
void nvs_add_uint8(nvs_handle_t *handle, char *key, uint8_t data);
void nvs_add_int8(nvs_handle_t *handle, char *key, int8_t data);
void nvs_add_uint16(nvs_handle_t *handle, char *key, uint16_t data);
void nvs_add_int16(nvs_handle_t *handle, char *key, int16_t data);
void nvs_add_uint32(nvs_handle_t *handle, char *key, uint32_t data);
void nvs_add_int32(nvs_handle_t *handle, char *key, int32_t data);
void nvs_add_uint64(nvs_handle_t *handle, char *key, uint64_t data);
void nvs_add_int64(nvs_handle_t *handle, char *key, int64_t data);
void nvs_add_float(nvs_handle_t *handle, char *key, float data);
void nvs_add_string(nvs_handle_t *handle, char *key, char *data);

// Commit data
void nvs_commit_data(nvs_handle_t *handle);

// NVS getters
bool nvs_get_uint8(char *namespace, char *key, uint8_t *data);
bool nvs_get_int8(char *namespace, char *key, int8_t *data);
bool nvs_get_uint16(char *namespace, char *key, uint16_t *data);
bool nvs_get_int16(char *namespace, char *key, int16_t *data);
bool nvs_get_uint32(char *namespace, char *key, uint32_t *data);
bool nvs_get_int32(char *namespace, char *key, int32_t *data);
bool nvs_get_uint64(char *namespace, char *key, uint64_t *data);
bool nvs_get_int64(char *namespace, char *key, int64_t *data);
bool nvs_get_float(char *namespace, char *key, float *data);
bool nvs_get_string(char *namespace, char *key, char *data);
