#include "observability_logger_writer.h"

#include "observability_logger_frame.h"
#include "observability_logger_queue.h"
#include "observability_logger_spool.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>

static int format_line_into(const observability_logger_slot_t* slot,
                            const char* category,
                            const char* payload,
                            char* line_buffer,
                            size_t line_buffer_capacity,
                            size_t* out_length) {
  int required = 0;
  if (line_buffer == NULL || out_length == NULL) {
    return 0;
  }
  required = snprintf(line_buffer,
                      line_buffer_capacity,
                      "%llu\t%llu\t%d\t%s\t%s\n",
                      (unsigned long long)slot->wall_time_unix_ms,
                      (unsigned long long)slot->sequence,
                      slot->level,
                      category,
                      payload);
  if (required <= 0 || (size_t)required >= line_buffer_capacity) {
    return 0;
  }
  *out_length = (size_t)required;
  return 1;
}

void* observability_logger_writer_main(void* opaque) {
  observability_logger_t* logger = (observability_logger_t*)opaque;

  /* The writer is the only thread that appends log lines and rotates active
   * segment files. Producers never touch disk; readBatch/ack can inspect files
   * under `mutex`, so segment changes stay serialized with delivery state. */
  for (;;) {
    observability_logger_slot_t slot;
    char* category_snapshot = logger->writer_category_snapshot;
    char* payload_snapshot = logger->writer_payload_snapshot;
    char* line = logger->writer_line_buffer;
    size_t line_length = 0u;
    int has_record = 0;
    uint32_t should_exit = 0u;

    pthread_mutex_lock(&logger->wake_mutex);
    has_record = observability_logger_queue_has_ready_record(logger);
    while (!has_record &&
           atomic_load_explicit(&logger->stop_requested, memory_order_acquire) == 0u) {
      pthread_cond_wait(&logger->not_empty, &logger->wake_mutex);
      has_record = observability_logger_queue_has_ready_record(logger);
    }

    if (!has_record &&
        atomic_load_explicit(&logger->stop_requested, memory_order_acquire) != 0u) {
      pthread_cond_broadcast(&logger->drained);
      pthread_mutex_unlock(&logger->wake_mutex);
      break;
    }
    pthread_mutex_unlock(&logger->wake_mutex);

    atomic_store_explicit(&logger->writer_in_flight, 1u, memory_order_release);
    if (!observability_logger_queue_try_dequeue_record(logger,
                                                       &slot,
                                                       category_snapshot,
                                                       payload_snapshot)) {
      atomic_store_explicit(&logger->writer_in_flight, 0u, memory_order_release);
      observability_logger_queue_broadcast_drained(logger);
      continue;
    }

    if (!format_line_into(&slot,
                          category_snapshot,
                          payload_snapshot,
                          line,
                          logger->writer_line_buffer_capacity,
                          &line_length)) {
      atomic_fetch_add_explicit(&logger->write_failure_count, 1u, memory_order_relaxed);
      atomic_store_explicit(&logger->writer_in_flight, 0u, memory_order_release);
      observability_logger_queue_broadcast_drained(logger);
      should_exit = atomic_load_explicit(&logger->stop_requested, memory_order_acquire) != 0u &&
                    observability_logger_queue_depth(logger) == 0u;
      if (should_exit != 0u) {
        break;
      }
      continue;
    }

    pthread_mutex_lock(&logger->mutex);
    if (logger->current_file_size_bytes > 0u &&
        logger->current_file_size_bytes + (uint64_t)line_length > logger->max_segment_size_bytes &&
        !observability_logger_spool_rotate_active_segment_locked(logger)) {
      atomic_fetch_add_explicit(&logger->write_failure_count, 1u, memory_order_relaxed);
      atomic_store_explicit(&logger->writer_in_flight, 0u, memory_order_release);
      observability_logger_queue_broadcast_drained(logger);
      should_exit = atomic_load_explicit(&logger->stop_requested, memory_order_acquire) != 0u &&
                    observability_logger_queue_depth(logger) == 0u;
      pthread_mutex_unlock(&logger->mutex);
      if (should_exit != 0u) {
        break;
      }
      continue;
    }

    /* Serialize active-file appends with readBatch/ack. Producers have already
     * handed the record to this background writer, so holding the spool mutex
     * during disk I/O does not put file syscalls back on the caller hotpath. */
    if (logger->active_fd >= 0 &&
        observability_logger_write_all_bytes(logger->active_fd, line, line_length)) {
      logger->current_file_size_bytes += (uint64_t)line_length;
      atomic_fetch_add_explicit(&logger->written_count, 1u, memory_order_relaxed);
      atomic_store_explicit(&logger->writer_in_flight, 0u, memory_order_release);
      should_exit = atomic_load_explicit(&logger->stop_requested, memory_order_acquire) != 0u &&
                    observability_logger_queue_depth(logger) == 0u;
      pthread_mutex_unlock(&logger->mutex);
      observability_logger_queue_broadcast_drained(logger);
      observability_logger_core_notify_data_available_once();
    } else {
      atomic_fetch_add_explicit(&logger->write_failure_count, 1u, memory_order_relaxed);
      atomic_store_explicit(&logger->writer_in_flight, 0u, memory_order_release);
      should_exit = atomic_load_explicit(&logger->stop_requested, memory_order_acquire) != 0u &&
                    observability_logger_queue_depth(logger) == 0u;
      pthread_mutex_unlock(&logger->mutex);
      observability_logger_queue_broadcast_drained(logger);
    }

    if (should_exit != 0u) {
      break;
    }
  }
  return NULL;
}
