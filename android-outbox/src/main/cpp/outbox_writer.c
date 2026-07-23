#include "outbox_writer.h"

#include "outbox_frame.h"
#include "outbox_queue.h"
#include "outbox_spool.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>

static int format_line_into(const outbox_slot_t* slot,
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

void* outbox_writer_main(void* opaque) {
  outbox_t* outbox = (outbox_t*)opaque;

  /* The writer is the only thread that appends log lines and rotates active
   * segment files. Producers never touch disk; readBatch/ack can inspect files
   * under `mutex`, so segment changes stay serialized with delivery state. */
  for (;;) {
    outbox_slot_t slot;
    char* category_snapshot = outbox->writer_category_snapshot;
    char* payload_snapshot = outbox->writer_payload_snapshot;
    char* line = outbox->writer_line_buffer;
    size_t line_length = 0u;
    int has_record = 0;
    uint32_t should_exit = 0u;

    pthread_mutex_lock(&outbox->wake_mutex);
    has_record = outbox_queue_has_ready_record(outbox);
    while (!has_record &&
           atomic_load_explicit(&outbox->stop_requested, memory_order_acquire) == 0u) {
      pthread_cond_wait(&outbox->not_empty, &outbox->wake_mutex);
      has_record = outbox_queue_has_ready_record(outbox);
    }

    if (!has_record &&
        atomic_load_explicit(&outbox->stop_requested, memory_order_acquire) != 0u) {
      pthread_cond_broadcast(&outbox->drained);
      pthread_mutex_unlock(&outbox->wake_mutex);
      break;
    }
    pthread_mutex_unlock(&outbox->wake_mutex);

    atomic_store_explicit(&outbox->writer_in_flight, 1u, memory_order_release);
    if (!outbox_queue_try_dequeue_record(outbox,
                                                       &slot,
                                                       category_snapshot,
                                                       payload_snapshot)) {
      atomic_store_explicit(&outbox->writer_in_flight, 0u, memory_order_release);
      outbox_queue_broadcast_drained(outbox);
      continue;
    }

    if (!format_line_into(&slot,
                          category_snapshot,
                          payload_snapshot,
                          line,
                          outbox->writer_line_buffer_capacity,
                          &line_length)) {
      atomic_fetch_add_explicit(&outbox->write_failure_count, 1u, memory_order_relaxed);
      atomic_store_explicit(&outbox->writer_in_flight, 0u, memory_order_release);
      outbox_queue_broadcast_drained(outbox);
      should_exit = atomic_load_explicit(&outbox->stop_requested, memory_order_acquire) != 0u &&
                    outbox_queue_depth(outbox) == 0u;
      if (should_exit != 0u) {
        break;
      }
      continue;
    }

    pthread_mutex_lock(&outbox->mutex);
    if (outbox->current_file_size_bytes > 0u &&
        outbox->current_file_size_bytes + (uint64_t)line_length > outbox->max_segment_size_bytes &&
        !outbox_spool_rotate_active_segment_locked(outbox)) {
      atomic_fetch_add_explicit(&outbox->write_failure_count, 1u, memory_order_relaxed);
      atomic_store_explicit(&outbox->writer_in_flight, 0u, memory_order_release);
      outbox_queue_broadcast_drained(outbox);
      should_exit = atomic_load_explicit(&outbox->stop_requested, memory_order_acquire) != 0u &&
                    outbox_queue_depth(outbox) == 0u;
      pthread_mutex_unlock(&outbox->mutex);
      if (should_exit != 0u) {
        break;
      }
      continue;
    }

    /* Serialize active-file appends with readBatch/ack. Producers have already
     * handed the record to this background writer, so holding the spool mutex
     * during disk I/O does not put file syscalls back on the caller hotpath. */
    if (outbox->active_fd >= 0 &&
        outbox_write_all_bytes(outbox->active_fd, line, line_length)) {
      outbox->current_file_size_bytes += (uint64_t)line_length;
      atomic_fetch_add_explicit(&outbox->written_count, 1u, memory_order_relaxed);
      atomic_store_explicit(&outbox->writer_in_flight, 0u, memory_order_release);
      should_exit = atomic_load_explicit(&outbox->stop_requested, memory_order_acquire) != 0u &&
                    outbox_queue_depth(outbox) == 0u;
      pthread_mutex_unlock(&outbox->mutex);
      outbox_queue_broadcast_drained(outbox);
      outbox_core_notify_data_available_once();
    } else {
      atomic_fetch_add_explicit(&outbox->write_failure_count, 1u, memory_order_relaxed);
      atomic_store_explicit(&outbox->writer_in_flight, 0u, memory_order_release);
      should_exit = atomic_load_explicit(&outbox->stop_requested, memory_order_acquire) != 0u &&
                    outbox_queue_depth(outbox) == 0u;
      pthread_mutex_unlock(&outbox->mutex);
      outbox_queue_broadcast_drained(outbox);
    }

    if (should_exit != 0u) {
      break;
    }
  }
  return NULL;
}
