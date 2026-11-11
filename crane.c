#include "crane.h"

#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include <esp_log.h>
#include <esp_timer.h>

#include <freertos/FreeRTOS.h>
#include <freertos/semphr.h>
#include <freertos/task.h>

#include <lownet.h>
#include <serial_io.h>
#include <utility.h>

#define CRANE_PROTO 0x05
#define TAG "crane"

#define MAX_PENDING 16

#define ACTION_TIMEOUT_MS       5000
#define TEST_ACTION_TIMEOUT_MS  2000
#define CLOSE_TIMEOUT_MS        5000
#define CONNECT_TIMEOUT_MS      5000
#define CLOSE_MAX_RETRIES       3

typedef struct {
    crane_packet_t pkt;
    uint32_t sent_ms;
    bool used;
    bool sent;
} pending_action_t;

static struct {
    uint8_t crane_id;

    bool connecting;
    bool connected;
    bool closing;
    bool test_mode;

    uint16_t seq;
    uint16_t ack_seq;
    uint8_t backlog;
    uint8_t time_left;
    uint8_t light;
    int8_t temp;

    pending_action_t pending[MAX_PENDING];

    uint32_t connect_sent_ms;

    uint16_t close_seq;
    uint32_t close_sent_ms;
    uint8_t close_retries;

    uint32_t action_timeout_ms;

    SemaphoreHandle_t lock;
    TaskHandle_t test_task;
} state;

static void crane_service_task(void* arg);
static void crane_test_task(void* arg);

static uint32_t now_ms(void)
{
    return (uint32_t)(esp_timer_get_time() / 1000);
}

static void crane_send_packet(const crane_packet_t* pkt)
{
    lownet_frame_t frame;
    memset(&frame, 0, sizeof(frame));

    frame.source = lownet_get_device_id();
    frame.destination = state.crane_id;
    frame.protocol = CRANE_PROTO;
    frame.length = sizeof(*pkt);

    memcpy(frame.payload, pkt, sizeof(*pkt));
    lownet_send(&frame);
}

static void pending_clear_locked(void)
{
    memset(state.pending, 0, sizeof(state.pending));
}

static bool pending_add_locked(const crane_packet_t* pkt)
{
    for (int i = 0; i < MAX_PENDING; ++i) {
        if (!state.pending[i].used) {
            state.pending[i].used = true;
            state.pending[i].sent = false;
            state.pending[i].pkt = *pkt;
            state.pending[i].sent_ms = 0;
            return true;
        }
    }
    return false;
}

static void pending_pump_locked(uint32_t t)
{
    int next = -1;

    for (int i = 0; i < MAX_PENDING; ++i) {
        if (!state.pending[i].used) {
            continue;
        }
        if (state.pending[i].sent) {
            return;
        }
        if (next < 0 || state.pending[i].pkt.seq < state.pending[next].pkt.seq) {
            next = i;
        }
    }

    if (next >= 0) {
        crane_send_packet(&state.pending[next].pkt);
        state.pending[next].sent = true;
        state.pending[next].sent_ms = t;
        ESP_LOGI(TAG, "Sent ACTION cmd=0x%02x seq=%u",
                 state.pending[next].pkt.d.action.cmd,
                 state.pending[next].pkt.seq);
    }
}

static void pending_ack_locked(uint16_t seq)
{
    for (int i = 0; i < MAX_PENDING; ++i) {
        if (state.pending[i].used && state.pending[i].pkt.seq <= seq) {
            state.pending[i].used = false;
        }
    }
}

static void pending_resend_seq_locked(uint16_t seq, uint32_t sent_ms)
{
    for (int i = 0; i < MAX_PENDING; ++i) {
        if (state.pending[i].used && state.pending[i].sent &&
            state.pending[i].pkt.seq == seq) {
            crane_send_packet(&state.pending[i].pkt);
            state.pending[i].sent_ms = sent_ms;
            ESP_LOGW(TAG, "Resent action seq=%u after NAK", seq);
            return;
        }
    }
}

static bool crane_connect_internal(uint8_t id, bool test)
{
    if (id == 0) {
        serial_write_line("Invalid crane node id");
        return false;
    }

    xSemaphoreTake(state.lock, portMAX_DELAY);

    if (state.connected || state.connecting || state.closing) {
        xSemaphoreGive(state.lock);
        serial_write_line("Crane already connected/connecting/closing");
        return false;
    }

    state.crane_id = id;
    state.connecting = true;
    state.connected = false;
    state.closing = false;
    state.test_mode = test;

    state.seq = 1;
    state.ack_seq = 0;
    state.backlog = 0;
    state.time_left = 0;
    state.light = 0;
    state.temp = 0;

    state.action_timeout_ms = test ? TEST_ACTION_TIMEOUT_MS : ACTION_TIMEOUT_MS;

    pending_clear_locked();

    crane_packet_t pkt;
    memset(&pkt, 0, sizeof(pkt));
    pkt.type = CRANE_CONNECT;
    pkt.flags = CRANE_SYN | (test ? CRANE_TEST : 0);
    pkt.seq = 0;
    pkt.d.conn.challenge = 0;

    state.connect_sent_ms = now_ms();
    crane_send_packet(&pkt);

    ESP_LOGI(TAG, "Sent CONNECT to 0x%02x%s",
             id, test ? " (TEST)" : "");

    xSemaphoreGive(state.lock);
    return true;
}

void crane_connect(uint8_t id)
{
    (void)crane_connect_internal(id, false);
}

void crane_disconnect(void)
{
    xSemaphoreTake(state.lock, portMAX_DELAY);

    if (!state.connected || state.closing) {
        xSemaphoreGive(state.lock);
        serial_write_line("Crane not connected");
        return;
    }

    crane_packet_t pkt;
    memset(&pkt, 0, sizeof(pkt));
    pkt.type = CRANE_CLOSE;
    pkt.flags = 0;
    pkt.seq = state.seq;
    pkt.d.close = 0;

    state.close_seq = state.seq;
    state.close_sent_ms = now_ms();
    state.close_retries = 0;
    state.closing = true;

    crane_send_packet(&pkt);

    ESP_LOGI(TAG, "Sent CLOSE seq=%u", state.close_seq);

    xSemaphoreGive(state.lock);
}

void crane_action(uint8_t action)
{
    xSemaphoreTake(state.lock, portMAX_DELAY);

    if (!state.connected || state.closing) {
        xSemaphoreGive(state.lock);
        serial_write_line("Crane not connected");
        return;
    }

    crane_packet_t pkt;
    memset(&pkt, 0, sizeof(pkt));
    pkt.type = CRANE_ACTION;
    pkt.flags = 0;
    pkt.seq = state.seq;
    pkt.d.action.cmd = action;

    if (!pending_add_locked(&pkt)) {
        xSemaphoreGive(state.lock);
        serial_write_line("Crane action queue full");
        return;
    }
    state.seq++;

    pending_pump_locked(now_ms());

    xSemaphoreGive(state.lock);
}

void crane_receive(const lownet_frame_t* frame)
{
    if (frame->length < sizeof(crane_packet_t)) {
        return;
    }

    crane_packet_t pkt;
    memcpy(&pkt, frame->payload, sizeof(pkt));

    xSemaphoreTake(state.lock, portMAX_DELAY);

    if (frame->source != state.crane_id) {
        xSemaphoreGive(state.lock);
        return;
    }

    switch (pkt.type) {
    case CRANE_CONNECT:
        if (pkt.flags & CRANE_NAK) {
            ESP_LOGW(TAG, "Crane rejected connection");
            state.connecting = false;
            state.connected = false;
            state.closing = false;
            state.test_mode = false;
        } else if ((pkt.flags & (CRANE_SYN | CRANE_ACK)) ==
                   (CRANE_SYN | CRANE_ACK) && state.connecting) {
            uint32_t response = ~pkt.d.conn.challenge;

            crane_packet_t ack;
            memset(&ack, 0, sizeof(ack));
            ack.type = CRANE_CONNECT;
            ack.flags = CRANE_ACK | (state.test_mode ? CRANE_TEST : 0);
            ack.seq = 0;
            ack.d.conn.challenge = response;

            crane_send_packet(&ack);

            state.connecting = false;
            state.connected = true;
            state.seq = 1;
            state.ack_seq = 0;
            state.backlog = 0;
            pending_clear_locked();

            ESP_LOGI(TAG, "Connected to crane 0x%02x%s",
                     state.crane_id, state.test_mode ? " (TEST)" : "");
        }
        break;

    case CRANE_STATUS:
        if (!state.connected) {
            break;
        }

        if (pkt.flags & CRANE_NAK) {
            ESP_LOGW(TAG, "Crane NAK for seq=%u", pkt.seq);
            pending_resend_seq_locked(pkt.seq, now_ms());
        } else {
            state.ack_seq = pkt.seq;
            pending_ack_locked(pkt.seq);
            pending_pump_locked(now_ms());
        }

        state.backlog = pkt.d.status.backlog;
        state.time_left = pkt.d.status.time_left;
        state.light = pkt.d.status.light;
        state.temp = pkt.d.status.temp;

        ESP_LOGD(TAG, "STATUS seq=%u backlog=%u time_left=%u light=%u temp=%d",
                 pkt.seq,
                 state.backlog,
                 state.time_left,
                 state.light,
                 state.temp);

        if (state.time_left == 0 && state.connected) {
            ESP_LOGW(TAG, "Crane control time expired");
            state.connected = false;
            state.closing = false;
            pending_clear_locked();
        }
        break;

    case CRANE_CLOSE:
        if (pkt.flags & CRANE_ACK) {
            ESP_LOGI(TAG, "Crane closed connection");
            state.connected = false;
            state.connecting = false;
            state.closing = false;
            state.test_mode = false;
            pending_clear_locked();
        } else if (pkt.flags & CRANE_NAK) {
            ESP_LOGW(TAG, "Crane NAK on CLOSE");
            state.connected = false;
            state.connecting = false;
            state.closing = false;
            pending_clear_locked();
        }
        break;

    default:
        break;
    }

    xSemaphoreGive(state.lock);
}

static void crane_service_task(void* arg)
{
    (void)arg;

    while (true) {
        vTaskDelay(pdMS_TO_TICKS(100));

        uint32_t t = now_ms();

        xSemaphoreTake(state.lock, portMAX_DELAY);

        if (state.connecting) {
            if (t - state.connect_sent_ms >= CONNECT_TIMEOUT_MS) {
                ESP_LOGW(TAG, "No connection answer from crane 0x%02x",
                         state.crane_id);
                state.connecting = false;
                state.test_mode = false;
            }
        } else if (state.closing) {
            if (t - state.close_sent_ms >= CLOSE_TIMEOUT_MS) {
                if (state.close_retries < CLOSE_MAX_RETRIES) {
                    crane_packet_t pkt;
                    memset(&pkt, 0, sizeof(pkt));
                    pkt.type = CRANE_CLOSE;
                    pkt.flags = 0;
                    pkt.seq = state.close_seq;
                    pkt.d.close = 0;

                    crane_send_packet(&pkt);

                    state.close_retries++;
                    state.close_sent_ms = t;

                    ESP_LOGW(TAG, "Resent CLOSE seq=%u retry=%u",
                             state.close_seq, state.close_retries);
                } else {
                    ESP_LOGE(TAG, "CLOSE failed after %u retries",
                             CLOSE_MAX_RETRIES);
                    state.closing = false;
                    state.connected = false;
                    pending_clear_locked();
                }
            }
        } else if (state.connected) {
            for (int i = 0; i < MAX_PENDING; ++i) {
                if (state.pending[i].used && state.pending[i].sent &&
                    (t - state.pending[i].sent_ms) >= state.action_timeout_ms) {
                    crane_send_packet(&state.pending[i].pkt);
                    state.pending[i].sent_ms = t;

                    ESP_LOGW(TAG, "Resent ACTION seq=%u",
                             state.pending[i].pkt.seq);
                }
            }
        }

        xSemaphoreGive(state.lock);
    }
}

static bool crane_wait_connected(uint32_t timeout_ms)
{
    uint32_t start = now_ms();

    while ((now_ms() - start) < timeout_ms) {
        bool connected;
        bool failed;

        xSemaphoreTake(state.lock, portMAX_DELAY);
        connected = state.connected;
        failed = (!state.connecting && !state.connected);
        xSemaphoreGive(state.lock);

        if (connected) {
            return true;
        }
        if (failed) {
            return false;
        }

        vTaskDelay(pdMS_TO_TICKS(100));
    }

    return false;
}

static bool crane_wait_done(uint16_t last_seq, uint32_t timeout_ms)
{
    uint32_t start = now_ms();

    while ((now_ms() - start) < timeout_ms) {
        bool done;
        bool dead;

        xSemaphoreTake(state.lock, portMAX_DELAY);
        done = state.connected &&
               state.ack_seq >= last_seq &&
               state.backlog == 0;
        dead = (!state.connected && !state.closing);
        xSemaphoreGive(state.lock);

        if (done) {
            return true;
        }
        if (dead) {
            return false;
        }

        vTaskDelay(pdMS_TO_TICKS(100));
    }

    return false;
}

static uint16_t crane_last_seq(void)
{
    uint16_t last;

    xSemaphoreTake(state.lock, portMAX_DELAY);
    last = state.seq - 1;
    xSemaphoreGive(state.lock);

    return last;
}

static void crane_test_task(void* arg)
{
    uint8_t id = (uint8_t)(uintptr_t)arg;
    bool ok = true;

    ESP_LOGI(TAG, "Starting test procedure to 0x%02x", id);

    if (!crane_connect_internal(id, true)) {
        ok = false;
    }

    if (ok && !crane_wait_connected(10000)) {
        ESP_LOGE(TAG, "Test: connection failed");
        ok = false;
    }

    if (ok) {
        crane_action(CRANE_LIGHT_ON);
        crane_action(CRANE_FWD);
        crane_action(CRANE_FWD);
        crane_action(CRANE_REV);

        uint16_t last = crane_last_seq();
        if (!crane_wait_done(last, 15000)) {
            ESP_LOGE(TAG, "Test: phase 1 did not complete");
            ok = false;
        }
    }

    if (ok) {
        crane_action(CRANE_DOWN);
        crane_action(CRANE_DOWN);

        uint16_t last = crane_last_seq();
        if (!crane_wait_done(last, 15000)) {
            ESP_LOGE(TAG, "Test: phase 2 did not complete");
            ok = false;
        }
    }

    if (ok) {
        crane_action(CRANE_UP);
        crane_action(CRANE_UP);
        crane_action(CRANE_REV);
        crane_action(CRANE_LIGHT_OFF);

        uint16_t last = crane_last_seq();
        if (!crane_wait_done(last, 15000)) {
            ESP_LOGE(TAG, "Test: phase 3 did not complete");
            ok = false;
        }
    }

    if (ok) {
        crane_disconnect();

        uint32_t start = now_ms();
        while ((now_ms() - start) < 10000) {
            bool closing;
            bool connected;

            xSemaphoreTake(state.lock, portMAX_DELAY);
            closing = state.closing;
            connected = state.connected;
            xSemaphoreGive(state.lock);

            if (!closing && !connected) {
                break;
            }

            vTaskDelay(pdMS_TO_TICKS(100));
        }
    } else {
        xSemaphoreTake(state.lock, portMAX_DELAY);
        bool connected = state.connected;
        xSemaphoreGive(state.lock);

        if (connected) {
            crane_disconnect();
        }
    }

    ESP_LOGI(TAG, "Test procedure %s", ok ? "finished" : "failed");

    xSemaphoreTake(state.lock, portMAX_DELAY);
    state.test_task = NULL;
    xSemaphoreGive(state.lock);

    vTaskDelete(NULL);
}

void crane_test(uint8_t id)
{
    xSemaphoreTake(state.lock, portMAX_DELAY);

    if (state.test_task != NULL) {
        xSemaphoreGive(state.lock);
        serial_write_line("Crane test already running");
        return;
    }

    xSemaphoreGive(state.lock);

    if (xTaskCreate(crane_test_task,
                    "crane_test",
                    4096,
                    (void*)(uintptr_t)id,
                    5,
                    &state.test_task) != pdPASS) {
        ESP_LOGE(TAG, "Failed to create crane test task");
        xSemaphoreTake(state.lock, portMAX_DELAY);
        state.test_task = NULL;
        xSemaphoreGive(state.lock);
    }
}

static uint8_t parse_id(const char* s)
{
    if (s == NULL) {
        return 0;
    }

    if (s[0] == '0' && (s[1] == 'x' || s[1] == 'X')) {
        s += 2;
    }

    uint32_t v = hex_to_dec(s);
    if (v == 0 || v > 0xFF) {
        return 0;
    }

    return (uint8_t)v;
}

static void crane_help(void)
{
    serial_write_line("/crane open [ID]   Connect to crane");
    serial_write_line("/crane close       Close connection");
    serial_write_line("/crane u|d|f|b     Up/down/forward/backward");
    serial_write_line("/crane o|O         Light on/off");
    serial_write_line("/crane test [ID]   Run test procedure");
}

int crane_init(void)
{
    memset(&state, 0, sizeof(state));

    state.lock = xSemaphoreCreateMutex();
    if (state.lock == NULL) {
        ESP_LOGE(TAG, "Failed to create crane mutex");
        return 1;
    }

    if (lownet_register_protocol(CRANE_PROTO, crane_receive) != 0) {
        ESP_LOGE(TAG, "Failed to register crane protocol");
        return 1;
    }

    if (xTaskCreate(crane_service_task,
                    "crane_service",
                    4096,
                    NULL,
                    4,
                    NULL) != pdPASS) {
        ESP_LOGE(TAG, "Failed to create crane service task");
        return 1;
    }

    return 0;
}

void crane_command(char* args)
{
    if (args == NULL || args[0] == '\0') {
        crane_help();
        return;
    }

    char* cmd = strtok(args, " ");
    if (cmd == NULL) {
        crane_help();
        return;
    }

    if (strcmp(cmd, "open") == 0) {
        char* id_str = strtok(NULL, " ");
        uint8_t id = parse_id(id_str);

        if (id == 0) {
            serial_write_line("Usage: /crane open [ID]");
            return;
        }

        crane_connect(id);
    } else if (strcmp(cmd, "close") == 0) {
        crane_disconnect();
    } else if (strcmp(cmd, "test") == 0) {
        char* id_str = strtok(NULL, " ");
        uint8_t id = parse_id(id_str);

        if (id == 0) {
            serial_write_line("Usage: /crane test [ID]");
            return;
        }

        crane_test(id);
    } else if (strlen(cmd) == 1) {
        switch (cmd[0]) {
        case 'u':
            crane_action(CRANE_UP);
            break;
        case 'd':
            crane_action(CRANE_DOWN);
            break;
        case 'f':
            crane_action(CRANE_FWD);
            break;
        case 'b':
            crane_action(CRANE_REV);
            break;
        case 'o':
            crane_action(CRANE_LIGHT_ON);
            break;
        case 'O':
            crane_action(CRANE_LIGHT_OFF);
            break;
        default:
            crane_help();
            break;
        }
    } else {
        crane_help();
    }
}
