#ifndef OUTBOX_CONTROL_H
#define OUTBOX_CONTROL_H

#include "outbox_core.h"

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

void outbox_control_notify_if_unacked_records_available(void);
#if defined(OUTBOX_TESTING)
outbox_status_t outbox_control_validate_command_frame(
    const uint8_t* frame,
    uint32_t frame_length);
#endif
void* outbox_control_main(void* opaque);

#ifdef __cplusplus
}
#endif

#endif
