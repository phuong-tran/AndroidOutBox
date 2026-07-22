#ifndef OBSERVABILITY_LOGGER_QUEUE_H
#define OBSERVABILITY_LOGGER_QUEUE_H

#include "observability_logger_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

uint32_t observability_logger_queue_depth(const observability_logger_t* logger);
void observability_logger_queue_signal_not_empty(observability_logger_t* logger);
void observability_logger_queue_broadcast_drained(observability_logger_t* logger);
void observability_logger_queue_leave_producer(observability_logger_t* logger);
int observability_logger_queue_try_enter_producer(observability_logger_t* logger);
int observability_logger_queue_has_ready_record(const observability_logger_t* logger);
int observability_logger_queue_try_enqueue_record(observability_logger_t* logger,
                                                  int32_t level,
                                                  const char* category,
                                                  uint32_t category_length,
                                                  const char* payload,
                                                  uint32_t payload_length,
                                                  uint64_t wall_time_unix_ms);
int observability_logger_queue_try_dequeue_record(observability_logger_t* logger,
                                                  observability_logger_slot_t* out_slot,
                                                  char* category_snapshot,
                                                  char* payload_snapshot);

#ifdef __cplusplus
}
#endif

#endif
