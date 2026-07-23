#ifndef OUTBOX_CONTROL_H
#define OUTBOX_CONTROL_H

#ifdef __cplusplus
extern "C" {
#endif

void outbox_control_notify_if_unacked_records_available(void);
void* outbox_control_main(void* opaque);

#ifdef __cplusplus
}
#endif

#endif
