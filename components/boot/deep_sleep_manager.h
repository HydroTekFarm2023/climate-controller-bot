#define ESP_INTR_FLAG_DEFAULT 0
#define GPIO_INPUT_PIN_SEL (1ULL << POWER_BUTTON_GPIO);

#define TIMER_DIVIDER         16  //  Hardware timer clock divider
#define TIMER_SCALE           (TIMER_BASE_CLK / TIMER_DIVIDER)  // convert counter value to seconds
#define BUTTON_PRESS_INTERVAL   (5)      // sample test interval for the first timer

#define DEEP_SLEEP_TAG "DEEP_SLEEP_MANAGER"

void init_power_button();
