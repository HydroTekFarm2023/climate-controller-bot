#include "nvs_manager.h"

#include <esp_err.h>
#include <esp_system.h>
#include <esp_event.h>
#include <esp_log.h>
#include <nvs_flash.h>
#include <stdlib.h>
#include <string.h>

void init_nvs() {
	// Check if space available in NVS, if not reset NVS
	esp_err_t ret = ret = nvs_flash_init();
	if (ret == ESP_ERR_NVS_NO_FREE_PAGES
			|| ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
		ESP_ERROR_CHECK(nvs_flash_erase());
		ret = nvs_flash_init();
	}
	ESP_ERROR_CHECK(ret);
}

void nvs_clear() {
	ESP_ERROR_CHECK(nvs_flash_erase());
	ESP_ERROR_CHECK(nvs_flash_init());
}

nvs_handle_t* nvs_get_handle(char *namespace) {
	nvs_handle_t *handle = malloc(sizeof(nvs_handle_t));
	nvs_open(namespace, NVS_READWRITE, handle);
	return handle;
}

void nvs_add_uint8(nvs_handle_t *handle, char *key, uint8_t data) {
	esp_err_t err = nvs_set_u8(*handle, key, data);

	if(err != ESP_OK) {
		if(err == ESP_ERR_NVS_NOT_ENOUGH_SPACE) {
			ESP_LOGE(NVS_TAG, "NOT ENOUGH STORAGE");
			// TODO take action, probably restart
		} else {
			ESP_LOGE(NVS_TAG, "Failed putting data in NVS, error:  %d", err);
		}
	}
}
void nvs_add_int8(nvs_handle_t *handle, char *key, int8_t data) {
	esp_err_t err = nvs_set_i8(*handle, key, data);

	if(err != ESP_OK) {
		if(err == ESP_ERR_NVS_NOT_ENOUGH_SPACE) {
			ESP_LOGE(NVS_TAG, "NOT ENOUGH STORAGE");
			// TODO take action, probably restart
		} else {
			ESP_LOGE(NVS_TAG, "Failed putting data in NVS, error:  %d", err);
		}
	}
}
void nvs_add_uint16(nvs_handle_t *handle, char *key, uint16_t data) {
	esp_err_t err = nvs_set_u16(*handle, key, data);

	if(err != ESP_OK) {
		if(err == ESP_ERR_NVS_NOT_ENOUGH_SPACE) {
			ESP_LOGE(NVS_TAG, "NOT ENOUGH STORAGE");
			// TODO take action, probably restart
		} else {
			ESP_LOGE(NVS_TAG, "Failed putting data in NVS, error:  %d", err);
		}
	}
}
void nvs_add_int16(nvs_handle_t *handle, char *key, int16_t data) {
	esp_err_t err = nvs_set_i16(*handle, key, data);

	if(err != ESP_OK) {
		if(err == ESP_ERR_NVS_NOT_ENOUGH_SPACE) {
			ESP_LOGE(NVS_TAG, "NOT ENOUGH STORAGE");
			// TODO take action, probably restart
		} else {
			ESP_LOGE(NVS_TAG, "Failed putting data in NVS, error:  %d", err);
		}
	}
}
void nvs_add_uint32(nvs_handle_t *handle, char *key, uint32_t data) {
	esp_err_t err = nvs_set_u32(*handle, key, data);

	if(err != ESP_OK) {
		if(err == ESP_ERR_NVS_NOT_ENOUGH_SPACE) {
			ESP_LOGE(NVS_TAG, "NOT ENOUGH STORAGE");
			// TODO take action, probably restart
		} else {
			ESP_LOGE(NVS_TAG, "Failed putting data in NVS, error:  %d", err);
		}
	}
}
void nvs_add_int32(nvs_handle_t *handle, char *key, int32_t data) {
	esp_err_t err = nvs_set_i32(*handle, key, data);

	if(err != ESP_OK) {
		if(err == ESP_ERR_NVS_NOT_ENOUGH_SPACE) {
			ESP_LOGE(NVS_TAG, "NOT ENOUGH STORAGE");
			// TODO take action, probably restart
		} else {
			ESP_LOGE(NVS_TAG, "Failed putting data in NVS, error:  %d", err);
		}
	}
}
void nvs_add_uint64(nvs_handle_t *handle, char *key, uint64_t data) {
	esp_err_t err = nvs_set_u64(*handle, key, data);

	if(err != ESP_OK) {
		if(err == ESP_ERR_NVS_NOT_ENOUGH_SPACE) {
			ESP_LOGE(NVS_TAG, "NOT ENOUGH STORAGE");
			// TODO take action, probably restart
		} else {
			ESP_LOGE(NVS_TAG, "Failed putting data in NVS, error:  %d", err);
		}
	}
}
void nvs_add_int64(nvs_handle_t *handle, char *key, int64_t data) {
	esp_err_t err = nvs_set_i64(*handle, key, data);

	if(err != ESP_OK) {
		if(err == ESP_ERR_NVS_NOT_ENOUGH_SPACE) {
			ESP_LOGE(NVS_TAG, "NOT ENOUGH STORAGE");
			// TODO take action, probably restart
		} else {
			ESP_LOGE(NVS_TAG, "Failed putting data in NVS, error:  %d", err);
		}
	}
}
void nvs_add_float(nvs_handle_t *handle, char *key, float data) {
	const size_t FLOAT_SIZE = 10;
	char float_str[FLOAT_SIZE];
	snprintf(float_str, FLOAT_SIZE, "%.2f", data);

	esp_err_t err = nvs_set_str(*handle, key, float_str);

	if(err != ESP_OK) {
		if(err == ESP_ERR_NVS_NOT_ENOUGH_SPACE) {
			ESP_LOGE(NVS_TAG, "NOT ENOUGH STORAGE");
			// TODO take action, probably restart
		} else {
			ESP_LOGE(NVS_TAG, "Failed putting data in NVS, error:  %d", err);
		}
	}
}
void nvs_add_string(nvs_handle_t *handle, char *key, char *data) {
	esp_err_t err = nvs_set_str(*handle, key, data);

	if(err != ESP_OK) {
		if(err == ESP_ERR_NVS_NOT_ENOUGH_SPACE) {
			ESP_LOGE(NVS_TAG, "NOT ENOUGH STORAGE");
			// TODO take action, probably restart
		} else {
			ESP_LOGE(NVS_TAG, "Failed putting data in NVS, error:  %d", err);
		}
	}
}

void nvs_commit_data(nvs_handle_t *handle) {
	esp_err_t err = nvs_commit(*handle);

	nvs_close(*handle);
	free(handle);

	if(err != ESP_OK) {
		ESP_LOGI(NVS_TAG, "Failed committing data to NVS");
	}
}

bool nvs_get_uint8(char *namespace, char *key, uint8_t *data) {
	nvs_handle_t handle;
	esp_err_t err = nvs_open(namespace, NVS_READONLY, &handle);
	if(err != ESP_OK) {
		ESP_LOGI(NVS_TAG, "Unable to open NVS");
		nvs_close(handle);
		return false;
	}

	err = nvs_get_u8(handle, key, data);

	nvs_close(handle);

	if(err != ESP_OK) {
		ESP_LOGI(NVS_TAG, "Failed getting data from NVS. Error: %d", err);
		return false;
	}

	return true;
}
bool nvs_get_int8(char *namespace, char *key, int8_t *data) {
	nvs_handle_t handle;
	esp_err_t err = nvs_open(namespace, NVS_READONLY, &handle);
	if(err != ESP_OK) {
		ESP_LOGI(NVS_TAG, "Unable to open NVS");
		nvs_close(handle);
		return false;
	}

	err = nvs_get_i8(handle, key, data);

	nvs_close(handle);

	if(err != ESP_OK) {
		ESP_LOGI(NVS_TAG, "Failed getting data from NVS. Error: %d", err);
		return false;
	}

	return true;
}
bool nvs_get_uint16(char *namespace, char *key, uint16_t *data) {
	nvs_handle_t handle;
	esp_err_t err = nvs_open(namespace, NVS_READONLY, &handle);
	if(err != ESP_OK) {
		ESP_LOGI(NVS_TAG, "Unable to open NVS");
		nvs_close(handle);
		return false;
	}

	err = nvs_get_u16(handle, key, data);

	nvs_close(handle);

	if(err != ESP_OK) {
		ESP_LOGI(NVS_TAG, "Failed getting data from NVS. Error: %d", err);
		return false;
	}

	return true;
}
bool nvs_get_int16(char *namespace, char *key, int16_t *data) {
	nvs_handle_t handle;
	esp_err_t err = nvs_open(namespace, NVS_READONLY, &handle);
	if(err != ESP_OK) {
		ESP_LOGI(NVS_TAG, "Unable to open NVS");
		nvs_close(handle);
		return false;
	}

	err = nvs_get_i16(handle, key, data);

	nvs_close(handle);

	if(err != ESP_OK) {
		ESP_LOGI(NVS_TAG, "Failed getting data from NVS. Error: %d", err);
		return false;
	}

	return true;
}
bool nvs_get_uint32(char *namespace, char *key, uint32_t *data) {
	nvs_handle_t handle;
	esp_err_t err = nvs_open(namespace, NVS_READONLY, &handle);
	if(err != ESP_OK) {
		ESP_LOGI(NVS_TAG, "Unable to open NVS");
		nvs_close(handle);
		return false;
	}

	err = nvs_get_u32(handle, key, data);

	nvs_close(handle);

	if(err != ESP_OK) {
		ESP_LOGI(NVS_TAG, "Failed getting data from NVS. Error: %d", err);
		return false;
	}

	return true;
}
bool nvs_get_int32(char *namespace, char *key, int32_t *data) {
	nvs_handle_t handle;
	esp_err_t err = nvs_open(namespace, NVS_READONLY, &handle);
	if(err != ESP_OK) {
		ESP_LOGI(NVS_TAG, "Unable to open NVS");
		nvs_close(handle);
		return false;
	}

	err = nvs_get_i32(handle, key, data);

	nvs_close(handle);

	if(err != ESP_OK) {
		ESP_LOGI(NVS_TAG, "Failed getting data from NVS. Error: %d", err);
		return false;
	}

	return true;
}
bool nvs_get_uint64(char *namespace, char *key, uint64_t *data) {
	nvs_handle_t handle;
	esp_err_t err = nvs_open(namespace, NVS_READONLY, &handle);
	if(err != ESP_OK) {
		ESP_LOGI(NVS_TAG, "Unable to open NVS");
		nvs_close(handle);
		return false;
	}

	err = nvs_get_u64(handle, key, data);

	nvs_close(handle);

	if(err != ESP_OK) {
		ESP_LOGI(NVS_TAG, "Failed getting data from NVS. Error: %d", err);
		return false;
	}

	return true;
}
bool nvs_get_int64(char *namespace, char *key, int64_t *data) {
	nvs_handle_t handle;
	esp_err_t err = nvs_open(namespace, NVS_READONLY, &handle);
	if(err != ESP_OK) {
		ESP_LOGI(NVS_TAG, "Unable to open NVS");
		nvs_close(handle);
		return false;
	}

	err = nvs_get_i64(handle, key, data);

	nvs_close(handle);

	if(err != ESP_OK) {
		ESP_LOGI(NVS_TAG, "Failed getting data from NVS. Error: %d", err);
		return false;
	}

	return true;
}
bool nvs_get_float(char *namespace, char *key, float *data) {
	nvs_handle_t handle;
	esp_err_t err = nvs_open(namespace, NVS_READONLY, &handle);
	if(err != ESP_OK) {
		ESP_LOGI(NVS_TAG, "Unable to open NVS");
		nvs_close(handle);
		return false;
	}

	size_t str_size;
	err = nvs_get_str(handle, key, NULL, &str_size);
	if(err != ESP_OK) {
		ESP_LOGI(NVS_TAG, "Failed getting data from NVS. Error: %d", err);
		nvs_close(handle);
		return false;
	}

	char *fl_str = malloc(str_size * sizeof(char));
	err = nvs_get_str(handle, key, fl_str, &str_size);
	nvs_close(handle);

	if(err != ESP_OK) {
		ESP_LOGI(NVS_TAG, "Failed getting data from NVS. Error: %d", err);
		free(fl_str);
		return false;
	}

	*data = atof(fl_str);
	free(fl_str);
	return true;
}
bool nvs_get_string(char *namespace, char *key, char *data) {
	nvs_handle_t handle;
	esp_err_t err = nvs_open(namespace, NVS_READONLY, &handle);
	if(err != ESP_OK) {
		ESP_LOGI(NVS_TAG, "Unable to open NVS");
		nvs_close(handle);
		return false;
	}

	size_t str_size;
	err = nvs_get_str(handle, key, NULL, &str_size);
	if(err != ESP_OK) {
		ESP_LOGI(NVS_TAG, "Failed getting data from NVS. Error: %d", err);
		nvs_close(handle);
		return false;
	}

	err = nvs_get_str(handle, key, data, &str_size);
	nvs_close(handle);

	if(err != ESP_OK) {
		ESP_LOGI(NVS_TAG, "Failed getting data from NVS. Error: %d", err);
		return false;
	}

	return true;
}
