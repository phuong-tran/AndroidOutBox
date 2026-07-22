#include "observability_logger_queue.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <string.h>

uint32_t observability_logger_queue_depth(const observability_logger_t* logger) {
  const uint64_t enqueue_cursor =
      atomic_load_explicit(&logger->enqueue_cursor, memory_order_acquire);
  const uint64_t dequeue_cursor =
      atomic_load_explicit(&logger->dequeue_cursor, memory_order_acquire);
  return (uint32_t)(enqueue_cursor - dequeue_cursor);
}

static uint32_t cursor_index(const observability_logger_t* logger, uint64_t cursor) {
  return (uint32_t)(cursor % (uint64_t)logger->queue_capacity);
}

static char* category_at(observability_logger_t* logger, uint32_t index) {
  return logger->categories + ((size_t)index * OBS_LOGGER_CATEGORY_SLOT_BYTES);
}

static char* payload_at(observability_logger_t* logger, uint32_t index) {
  return logger->payloads + ((size_t)index * logger->payload_slot_bytes);
}

static void copy_text_sanitized(char* dst,
                                uint32_t dst_capacity,
                                const char* src,
                                uint32_t length) {
  uint32_t index = 0u;
  if (dst_capacity == 0u) {
    return;
  }
  for (index = 0u; index < length && index + 1u < dst_capacity; ++index) {
    const char c = src[index];
    dst[index] = (c == '\n' || c == '\r' || c == '\t') ? ' ' : c;
  }
  dst[index] = '\0';
}

static void update_queue_high_watermark(observability_logger_t* logger, uint32_t depth) {
  uint32_t current = atomic_load_explicit(&logger->queue_high_watermark, memory_order_relaxed);
  while (depth > current &&
         !atomic_compare_exchange_weak_explicit(&logger->queue_high_watermark,
                                                &current,
                                                depth,
                                                memory_order_relaxed,
                                                memory_order_relaxed)) {
  }
}

void observability_logger_queue_signal_not_empty(observability_logger_t* logger) {
  pthread_mutex_lock(&logger->wake_mutex);
  pthread_cond_signal(&logger->not_empty);
  pthread_mutex_unlock(&logger->wake_mutex);
}

void observability_logger_queue_broadcast_drained(observability_logger_t* logger) {
  pthread_mutex_lock(&logger->wake_mutex);
  pthread_cond_broadcast(&logger->drained);
  pthread_mutex_unlock(&logger->wake_mutex);
}

void observability_logger_queue_leave_producer(observability_logger_t* logger) {
  if (atomic_fetch_sub_explicit(&logger->active_producer_count, 1u, memory_order_acq_rel) == 1u &&
      atomic_load_explicit(&logger->stop_requested, memory_order_acquire) != 0u) {
    pthread_mutex_lock(&logger->wake_mutex);
    pthread_cond_broadcast(&logger->producers_drained);
    pthread_mutex_unlock(&logger->wake_mutex);
  }
}

int observability_logger_queue_try_enter_producer(observability_logger_t* logger) {
  if (atomic_load_explicit(&logger->started, memory_order_acquire) == 0u ||
      atomic_load_explicit(&logger->stop_requested, memory_order_acquire) != 0u) {
    return 0;
  }

  atomic_fetch_add_explicit(&logger->active_producer_count, 1u, memory_order_acq_rel);
  if (atomic_load_explicit(&logger->started, memory_order_acquire) == 0u ||
      atomic_load_explicit(&logger->stop_requested, memory_order_acquire) != 0u) {
    observability_logger_queue_leave_producer(logger);
    return 0;
  }
  return 1;
}

int observability_logger_queue_has_ready_record(const observability_logger_t* logger) {
  const uint64_t tail = atomic_load_explicit(&logger->dequeue_cursor, memory_order_relaxed);
  const uint32_t index = cursor_index(logger, tail);
  const uint64_t state_sequence =
      atomic_load_explicit(&logger->slots[index].state_sequence, memory_order_acquire);
  /* Acquire pairs with the producer's release store after it has copied the
   * category/payload bytes. Seeing `tail + 1` means the writer can read the
   * slot metadata and side buffers without an additional lock. */
  return state_sequence == tail + 1u ? 1 : 0;
}

int observability_logger_queue_try_enqueue_record(observability_logger_t* logger,
                                                  int32_t level,
                                                  const char* category,
                                                  uint32_t category_length,
                                                  const char* payload,
                                                  uint32_t payload_length,
                                                  uint64_t wall_time_unix_ms) {
  uint64_t head = atomic_load_explicit(&logger->enqueue_cursor, memory_order_relaxed);
  uint32_t index = 0u;
  observability_logger_slot_t* slot = NULL;

  /* Bounded MPSC enqueue. The CAS reserves exactly one slot; record bytes are
   * copied only after ownership is established, so producer threads never share
   * a writable slot. */
  for (;;) {
    uint64_t state_sequence = 0u;
    int64_t diff = 0;

    index = cursor_index(logger, head);
    slot = &logger->slots[index];
    state_sequence = atomic_load_explicit(&slot->state_sequence, memory_order_acquire);
    diff = (int64_t)(state_sequence - head);
    if (diff == 0) {
      uint64_t desired = head + 1u;
      if (atomic_compare_exchange_weak_explicit(&logger->enqueue_cursor,
                                                &head,
                                                desired,
                                                memory_order_acq_rel,
                                                memory_order_relaxed)) {
        break;
      }
      continue;
    }
    if (diff < 0) {
      return 0;
    }
    head = atomic_load_explicit(&logger->enqueue_cursor, memory_order_relaxed);
  }

  copy_text_sanitized(category_at(logger, index),
                      OBS_LOGGER_CATEGORY_CAPACITY,
                      category,
                      category_length);
  copy_text_sanitized(payload_at(logger, index),
                      logger->max_record_bytes,
                      payload,
                      payload_length);

  slot->sequence = atomic_fetch_add_explicit(&logger->next_sequence, 1u, memory_order_relaxed);
  slot->wall_time_unix_ms = wall_time_unix_ms;
  slot->level = level;
  slot->category_length = category_length;
  slot->payload_length = payload_length;
  atomic_store_explicit(&slot->state_sequence, head + 1u, memory_order_release);
  update_queue_high_watermark(logger, observability_logger_queue_depth(logger));
  return 1;
}

int observability_logger_queue_try_dequeue_record(observability_logger_t* logger,
                                                  observability_logger_slot_t* out_slot,
                                                  char* category_snapshot,
                                                  char* payload_snapshot) {
  const uint64_t tail = atomic_load_explicit(&logger->dequeue_cursor, memory_order_relaxed);
  const uint32_t index = cursor_index(logger, tail);
  observability_logger_slot_t* slot = &logger->slots[index];
  const uint64_t state_sequence =
      atomic_load_explicit(&slot->state_sequence, memory_order_acquire);

  if (state_sequence != tail + 1u) {
    return 0;
  }

  out_slot->sequence = slot->sequence;
  out_slot->wall_time_unix_ms = slot->wall_time_unix_ms;
  out_slot->level = slot->level;
  out_slot->category_length = slot->category_length;
  out_slot->payload_length = slot->payload_length;
  memcpy(category_snapshot, category_at(logger, index), out_slot->category_length + 1u);
  memcpy(payload_snapshot, payload_at(logger, index), out_slot->payload_length + 1u);
  /* The writer is the only consumer. Advancing dequeue_cursor publishes queue
   * progress; advancing the slot sequence after that hands this slot back to a
   * future producer reservation. */
  atomic_store_explicit(&logger->dequeue_cursor, tail + 1u, memory_order_release);
  atomic_store_explicit(&slot->state_sequence,
                        tail + (uint64_t)logger->queue_capacity,
                        memory_order_release);
  return 1;
}
