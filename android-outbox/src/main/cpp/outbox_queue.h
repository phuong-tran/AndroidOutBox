#ifndef OUTBOX_QUEUE_H
#define OUTBOX_QUEUE_H

#include "outbox_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

uint32_t outbox_queue_depth(const outbox_t* outbox);
void outbox_queue_signal_not_empty(outbox_t* outbox);
void outbox_queue_broadcast_drained(outbox_t* outbox);
void outbox_queue_leave_producer(outbox_t* outbox);
int outbox_queue_try_enter_producer(outbox_t* outbox);
int outbox_queue_has_ready_record(const outbox_t* outbox);
int outbox_queue_try_enqueue_record(outbox_t* outbox,
                                                  int32_t level,
                                                  const char* category,
                                                  uint32_t category_length,
                                                  const char* payload,
                                                  uint32_t payload_length,
                                                  uint64_t wall_time_unix_ms);
int outbox_queue_try_dequeue_record(outbox_t* outbox,
                                                  outbox_slot_t* out_slot,
                                                  char* category_snapshot,
                                                  char* payload_snapshot);

#ifdef __cplusplus
}
#endif

#endif
