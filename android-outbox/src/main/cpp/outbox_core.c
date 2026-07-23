#include "outbox_core.h"
#include "outbox_control.h"
#include "outbox_frame.h"
#include "outbox_internal.h"
#include "outbox_queue.h"
#include "outbox_spool.h"
#include "outbox_writer.h"

#include <errno.h>
#include <pthread.h>
#include <stddef.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define OUTBOX_DOORBELL_HANDSHAKE 0u
#define OUTBOX_DOORBELL_DATA_AVAILABLE 1u
#define OUTBOX_DOORBELL_DROPPED_RECORD 2u
#define OUTBOX_DOORBELL_PAYLOAD_BYTES 4u
#define OUTBOX_DOORBELL_FRAME_BYTES \
  (OUTBOX_FRAME_HEADER_BYTES + OUTBOX_DOORBELL_PAYLOAD_BYTES)

/* This file deliberately keeps the native side as a small process-local service.
 *
 * There are two data paths:
 *   1. Hot log path: app threads enqueue fixed-size native slots. No JNI object
 *      construction and no file I/O happen on those caller threads.
 *   2. Control/delivery path: Kotlin sends length-prefixed command frames over a
 *      pipe. Native replies on a separate pipe and sends small doorbell frames
 *      when Kotlin should wake up.
 *
 * Segment files are the source of truth after the writer persists a record. The
 * per-provider ack cursors are stored as text so crash recovery can resume each
 * consumer from the last delivered byte offset without parsing any Kotlin state.
 */

outbox_t outbox_state = {
    .mutex = PTHREAD_MUTEX_INITIALIZER,
    .wake_mutex = PTHREAD_MUTEX_INITIALIZER,
    .not_empty = PTHREAD_COND_INITIALIZER,
    .drained = PTHREAD_COND_INITIALIZER,
    .producers_drained = PTHREAD_COND_INITIALIZER,
    .command_read_fd = -1,
    .doorbell_write_fd = -1,
    .record_write_fd = -1,
    .active_fd = -1,
};

static pthread_mutex_t outbox_stop_mutex = PTHREAD_MUTEX_INITIALIZER;

static uint64_t now_wall_time_unix_ms(void) {
  struct timespec ts;
  if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
    return 0u;
  }
  return ((uint64_t)ts.tv_sec * 1000ull) + ((uint64_t)ts.tv_nsec / 1000000ull);
}

static uint32_t bounded_strlen(const char* text, uint32_t limit) {
  uint32_t length = 0u;
  if (text == NULL) {
    return 0u;
  }
  while (length < limit && text[length] != '\0') {
    ++length;
  }
  return length;
}

static size_t align_up_size(size_t value, size_t alignment) {
  const size_t remainder = value % alignment;
  return remainder == 0u ? value : value + alignment - remainder;
}

static void* zalloc_cacheline_aligned(size_t size) {
  void* data = NULL;
  if (size == 0u || posix_memalign(&data, OUTBOX_CACHELINE, size) != 0) {
    return NULL;
  }
  memset(data, 0, size);
  return data;
}

static int write_doorbell_frame_to_fd(int fd, uint32_t event) {
  uint8_t frame[OUTBOX_DOORBELL_FRAME_BYTES] = {};
  outbox_write_u32_be(OUTBOX_DOORBELL_PAYLOAD_BYTES, frame);
  outbox_write_u32_le(event, frame + OUTBOX_FRAME_HEADER_BYTES);

  for (uint32_t attempt = 0u; attempt < 2u; attempt++) {
    if (outbox_write_pipe_all_bytes(fd, frame, sizeof(frame))) {
      return 1;
    }
    if (errno == EINTR) {
      continue;
    }
    break;
  }
  return 0;
}

static void notify_doorbell(uint32_t event) {
  /* Keep the fd write under the outbox mutex so stop/close cannot race with a
   * reused fd. The frame is fixed at 8 bytes and the pipe is nonblocking, so this
   * critical section stays bounded on the log writer path. */
  pthread_mutex_lock(&outbox_state.mutex);
  if (outbox_state.doorbell_write_fd >= 0) {
    write_doorbell_frame_to_fd(outbox_state.doorbell_write_fd, event);
  }
  pthread_mutex_unlock(&outbox_state.mutex);
}

void outbox_core_notify_data_available_once(void) {
  pthread_mutex_lock(&outbox_state.mutex);
  /* DATA_AVAILABLE is a doorbell, not a counter. Keep only one pending wakeup
   * until Kotlin asks for a batch; this avoids flooding the pipe during bursts
   * while still preserving durable records in segment files. */
  if (outbox_state.doorbell_write_fd >= 0 &&
      outbox_state.data_available_doorbell_pending == 0u &&
      write_doorbell_frame_to_fd(outbox_state.doorbell_write_fd,
                                 OUTBOX_DOORBELL_DATA_AVAILABLE)) {
    outbox_state.data_available_doorbell_pending = 1u;
  }
  pthread_mutex_unlock(&outbox_state.mutex);
}

static void release_storage_locked(outbox_t* outbox) {
  if (outbox->active_fd >= 0) {
    close(outbox->active_fd);
  }
  free(outbox->spool_directory_path);
  free(outbox->active_file_path);
  free(outbox->cursor_directory_path);
  for (uint32_t index = 0u; index < outbox->provider_cursor_count; ++index) {
    free(outbox->provider_cursors[index].cursor_file_path);
  }
  free(outbox->slots);
  free(outbox->categories);
  free(outbox->payloads);
  free(outbox->writer_category_snapshot);
  free(outbox->writer_payload_snapshot);
  free(outbox->writer_line_buffer);
  outbox->spool_directory_path = NULL;
  outbox->active_file_path = NULL;
  outbox->cursor_directory_path = NULL;
  outbox->active_fd = -1;
  outbox->active_segment_id = 0u;
  outbox->provider_cursor_count = 0u;
  memset(outbox->provider_cursors, 0, sizeof(outbox->provider_cursors));
  outbox->slots = NULL;
  outbox->categories = NULL;
  outbox->payloads = NULL;
  outbox->writer_category_snapshot = NULL;
  outbox->writer_payload_snapshot = NULL;
  outbox->writer_line_buffer = NULL;
  outbox->writer_line_buffer_capacity = 0u;
  outbox->payload_slot_bytes = 0u;
}

outbox_status_t outbox_open_pipes(
    outbox_pipes_t* out_pipes) {
  int command_pipe[2] = {-1, -1};
  int doorbell_pipe[2] = {-1, -1};
  int record_pipe[2] = {-1, -1};
  int kotlin_write_fd = -1;
  int kotlin_read_fd = -1;
  int kotlin_record_read_fd = -1;
  int pthread_result = 0;

  /* Pipe layout:
   *   command:  Kotlin writes command frames, native control thread reads.
   *   doorbell: native writes small wakeup frames, Kotlin reads.
   *   record:   native writes command responses and batch payloads, Kotlin reads.
   *
   * The command read fd and native write fds stay in outbox_state. Kotlin receives
   * duplicated peer fds, making close ownership explicit on both sides. */
  if (out_pipes == NULL) {
    return OUTBOX_STATUS_INVALID_ARGUMENT;
  }
  out_pipes->command_write_fd = -1;
  out_pipes->doorbell_read_fd = -1;
  out_pipes->record_read_fd = -1;

  pthread_mutex_lock(&outbox_state.mutex);
  if (outbox_state.control_thread_started != 0u || outbox_state.command_read_fd != -1 ||
      outbox_state.doorbell_write_fd != -1 || outbox_state.record_write_fd != -1) {
    pthread_mutex_unlock(&outbox_state.mutex);
    return OUTBOX_STATUS_BAD_STATE;
  }
  pthread_mutex_unlock(&outbox_state.mutex);

  if (pipe(command_pipe) != 0 || pipe(doorbell_pipe) != 0 || pipe(record_pipe) != 0) {
    if (command_pipe[0] != -1) {
      close(command_pipe[0]);
    }
    if (command_pipe[1] != -1) {
      close(command_pipe[1]);
    }
    if (doorbell_pipe[0] != -1) {
      close(doorbell_pipe[0]);
    }
    if (doorbell_pipe[1] != -1) {
      close(doorbell_pipe[1]);
    }
    if (record_pipe[0] != -1) {
      close(record_pipe[0]);
    }
    if (record_pipe[1] != -1) {
      close(record_pipe[1]);
    }
    return OUTBOX_STATUS_INTERNAL_ERROR;
  }
  if (!outbox_set_fd_cloexec(command_pipe[0]) ||
      !outbox_set_fd_cloexec(command_pipe[1]) ||
      !outbox_set_fd_cloexec(doorbell_pipe[0]) ||
      !outbox_set_fd_cloexec(doorbell_pipe[1]) ||
      !outbox_set_fd_cloexec(record_pipe[0]) ||
      !outbox_set_fd_cloexec(record_pipe[1]) ||
      !outbox_set_fd_nonblocking(command_pipe[0]) ||
      !outbox_set_fd_nonblocking(doorbell_pipe[1])) {
    close(command_pipe[0]);
    close(command_pipe[1]);
    close(doorbell_pipe[0]);
    close(doorbell_pipe[1]);
    close(record_pipe[0]);
    close(record_pipe[1]);
    return OUTBOX_STATUS_INTERNAL_ERROR;
  }

  kotlin_write_fd = dup(command_pipe[1]);
  close(command_pipe[1]);
  kotlin_read_fd = dup(doorbell_pipe[0]);
  close(doorbell_pipe[0]);
  kotlin_record_read_fd = dup(record_pipe[0]);
  close(record_pipe[0]);
  if (kotlin_write_fd < 0 || kotlin_read_fd < 0 || kotlin_record_read_fd < 0) {
    close(command_pipe[0]);
    close(doorbell_pipe[1]);
    close(record_pipe[1]);
    if (kotlin_write_fd >= 0) {
      close(kotlin_write_fd);
    }
    if (kotlin_read_fd >= 0) {
      close(kotlin_read_fd);
    }
    if (kotlin_record_read_fd >= 0) {
      close(kotlin_record_read_fd);
    }
    return OUTBOX_STATUS_INTERNAL_ERROR;
  }
  if (!outbox_set_fd_cloexec(kotlin_write_fd) ||
      !outbox_set_fd_cloexec(kotlin_read_fd) ||
      !outbox_set_fd_cloexec(kotlin_record_read_fd)) {
    close(command_pipe[0]);
    close(doorbell_pipe[1]);
    close(record_pipe[1]);
    close(kotlin_write_fd);
    close(kotlin_read_fd);
    close(kotlin_record_read_fd);
    return OUTBOX_STATUS_INTERNAL_ERROR;
  }

  pthread_mutex_lock(&outbox_state.mutex);
  outbox_state.command_read_fd = command_pipe[0];
  outbox_state.doorbell_write_fd = doorbell_pipe[1];
  outbox_state.record_write_fd = record_pipe[1];
  outbox_state.data_available_doorbell_pending = 0u;
  outbox_state.control_running = 1u;
  outbox_state.control_thread_started = 0u;
  pthread_result = pthread_create(&outbox_state.control_thread,
                                  NULL,
                                  outbox_control_main,
                                  &outbox_state);
  if (pthread_result != 0) {
    outbox_state.command_read_fd = -1;
    outbox_state.doorbell_write_fd = -1;
    outbox_state.record_write_fd = -1;
    outbox_state.control_running = 0u;
    pthread_mutex_unlock(&outbox_state.mutex);
    close(command_pipe[0]);
    close(doorbell_pipe[1]);
    close(record_pipe[1]);
    close(kotlin_write_fd);
    close(kotlin_read_fd);
    close(kotlin_record_read_fd);
    return OUTBOX_STATUS_INTERNAL_ERROR;
  }
  outbox_state.control_thread_started = 1u;
  pthread_mutex_unlock(&outbox_state.mutex);

  out_pipes->command_write_fd = kotlin_write_fd;
  out_pipes->doorbell_read_fd = kotlin_read_fd;
  out_pipes->record_read_fd = kotlin_record_read_fd;
  notify_doorbell(OUTBOX_DOORBELL_HANDSHAKE);
  outbox_control_notify_if_unacked_records_available();
  return OUTBOX_STATUS_OK;
}

void outbox_close_pipes(void) {
  pthread_t control_thread;
  uint32_t should_join = 0u;
  int command_read_fd = -1;
  int doorbell_write_fd = -1;
  int record_write_fd = -1;

  pthread_mutex_lock(&outbox_state.mutex);
  outbox_state.control_running = 0u;
  command_read_fd = outbox_state.command_read_fd;
  doorbell_write_fd = outbox_state.doorbell_write_fd;
  record_write_fd = outbox_state.record_write_fd;
  outbox_state.command_read_fd = -1;
  outbox_state.doorbell_write_fd = -1;
  outbox_state.record_write_fd = -1;
  outbox_state.data_available_doorbell_pending = 0u;
  should_join = outbox_state.control_thread_started;
  control_thread = outbox_state.control_thread;
  pthread_mutex_unlock(&outbox_state.mutex);

  if (command_read_fd != -1) {
    close(command_read_fd);
  }
  if (doorbell_write_fd != -1) {
    close(doorbell_write_fd);
  }
  if (record_write_fd != -1) {
    close(record_write_fd);
  }
  if (should_join != 0u && !pthread_equal(pthread_self(), control_thread)) {
    pthread_join(control_thread, NULL);
  }

  pthread_mutex_lock(&outbox_state.mutex);
  outbox_state.control_thread_started = 0u;
  pthread_mutex_unlock(&outbox_state.mutex);
}

outbox_status_t outbox_start(
    const outbox_config_t* config) {
  uint32_t queue_capacity = OUTBOX_DEFAULT_QUEUE_CAPACITY;
  uint32_t max_record_bytes = OUTBOX_DEFAULT_RECORD_BYTES;
  uint64_t max_segment_size_bytes = OUTBOX_DEFAULT_SEGMENT_SIZE_BYTES;
  uint32_t max_archived_segments = 0u;
  int pthread_result = 0;
  uint64_t min_segment_id = 0u;
  uint64_t max_segment_id = 0u;
  uint32_t segment_count = 0u;

  /* Start can be called directly by C tests or indirectly from the pipe control
   * protocol. It rebuilds all in-memory state from the spool directory and
   * cursor file before accepting producers. */
  if (config == NULL || config->spool_directory_path == NULL ||
      config->spool_directory_path[0] == '\0') {
    return OUTBOX_STATUS_INVALID_ARGUMENT;
  }

  pthread_mutex_lock(&outbox_state.mutex);
  if (atomic_load_explicit(&outbox_state.started, memory_order_acquire) != 0u) {
    pthread_mutex_unlock(&outbox_state.mutex);
    return OUTBOX_STATUS_BAD_STATE;
  }

  queue_capacity = config->queue_capacity == 0u ? queue_capacity : config->queue_capacity;
  max_record_bytes = config->max_record_bytes == 0u ? max_record_bytes : config->max_record_bytes;
  max_segment_size_bytes = config->max_segment_size_bytes == 0u ? max_segment_size_bytes
                                                                : config->max_segment_size_bytes;
  max_archived_segments = config->max_archived_segments;
  if (queue_capacity == 0u ||
      max_record_bytes == 0u ||
      max_segment_size_bytes == 0u) {
    pthread_mutex_unlock(&outbox_state.mutex);
    return OUTBOX_STATUS_INVALID_ARGUMENT;
  }
  if ((size_t)queue_capacity > SIZE_MAX / sizeof(*outbox_state.slots) ||
      (size_t)queue_capacity > SIZE_MAX / OUTBOX_CATEGORY_SLOT_BYTES) {
    pthread_mutex_unlock(&outbox_state.mutex);
    return OUTBOX_STATUS_INVALID_ARGUMENT;
  }

  atomic_store_explicit(&outbox_state.stop_requested, 0u, memory_order_release);
  atomic_store_explicit(&outbox_state.writer_in_flight, 0u, memory_order_release);
  outbox_state.writer_thread_started = 0u;
  outbox_state.queue_capacity = queue_capacity;
  outbox_state.max_record_bytes = max_record_bytes;
  outbox_state.max_segment_size_bytes = max_segment_size_bytes;
  outbox_state.max_archived_segments = max_archived_segments;
  atomic_store_explicit(&outbox_state.enqueue_cursor, 0u, memory_order_release);
  atomic_store_explicit(&outbox_state.dequeue_cursor, 0u, memory_order_release);
  atomic_store_explicit(&outbox_state.active_producer_count, 0u, memory_order_release);
  atomic_store_explicit(&outbox_state.next_sequence, 0u, memory_order_release);
  atomic_store_explicit(&outbox_state.queue_high_watermark, 0u, memory_order_release);
  atomic_store_explicit(&outbox_state.accepted_count, 0u, memory_order_release);
  atomic_store_explicit(&outbox_state.written_count, 0u, memory_order_release);
  atomic_store_explicit(&outbox_state.dropped_queue_full_count, 0u, memory_order_release);
  atomic_store_explicit(&outbox_state.dropped_invalid_count, 0u, memory_order_release);
  atomic_store_explicit(&outbox_state.dropped_record_too_large_count, 0u, memory_order_release);
  atomic_store_explicit(&outbox_state.write_failure_count, 0u, memory_order_release);
  outbox_state.data_available_doorbell_pending = 0u;
  outbox_state.current_file_size_bytes = 0u;
  outbox_state.roll_count = 0u;
  outbox_state.active_fd = -1;
  outbox_state.writer_line_buffer_capacity =
      outbox_spool_line_buffer_capacity(max_record_bytes);
  outbox_state.payload_slot_bytes = align_up_size((size_t)max_record_bytes, OUTBOX_CACHELINE);
  if (outbox_state.payload_slot_bytes == 0u ||
      (size_t)queue_capacity > SIZE_MAX / outbox_state.payload_slot_bytes) {
    pthread_mutex_unlock(&outbox_state.mutex);
    return OUTBOX_STATUS_INVALID_ARGUMENT;
  }
  if (!outbox_spool_ensure_directory(config->spool_directory_path)) {
    pthread_mutex_unlock(&outbox_state.mutex);
    return OUTBOX_STATUS_INVALID_ARGUMENT;
  }

  outbox_state.spool_directory_path = strdup(config->spool_directory_path);
  outbox_state.cursor_directory_path =
      outbox_spool_cursor_directory_path(config->spool_directory_path);
  if (outbox_state.cursor_directory_path != NULL &&
      !outbox_spool_ensure_directory(outbox_state.cursor_directory_path)) {
    release_storage_locked(&outbox_state);
    pthread_mutex_unlock(&outbox_state.mutex);
    return OUTBOX_STATUS_INTERNAL_ERROR;
  }
  if (!outbox_spool_scan_segment_bounds(config->spool_directory_path,
                           &min_segment_id,
                           &max_segment_id,
                           &segment_count)) {
    release_storage_locked(&outbox_state);
    pthread_mutex_unlock(&outbox_state.mutex);
    return OUTBOX_STATUS_INTERNAL_ERROR;
  }
  outbox_state.active_segment_id = segment_count == 0u ? 1u : max_segment_id;
  outbox_state.active_file_path =
      outbox_spool_segment_path(config->spool_directory_path,
                                              outbox_state.active_segment_id);
  outbox_state.active_fd =
      outbox_spool_open_segment_append_fd(outbox_state.active_file_path);
  outbox_state.slots =
      (outbox_slot_t*)zalloc_cacheline_aligned(
          (size_t)queue_capacity * sizeof(*outbox_state.slots));
  outbox_state.categories =
      (char*)zalloc_cacheline_aligned((size_t)queue_capacity * OUTBOX_CATEGORY_SLOT_BYTES);
  outbox_state.payloads =
      (char*)zalloc_cacheline_aligned((size_t)queue_capacity * outbox_state.payload_slot_bytes);
  outbox_state.writer_category_snapshot = (char*)calloc(1u, OUTBOX_CATEGORY_CAPACITY);
  outbox_state.writer_payload_snapshot = (char*)calloc(1u, (size_t)max_record_bytes);
  outbox_state.writer_line_buffer = (char*)calloc(1u, outbox_state.writer_line_buffer_capacity);
  if (outbox_state.spool_directory_path == NULL || outbox_state.active_file_path == NULL ||
      outbox_state.cursor_directory_path == NULL || outbox_state.active_fd < 0 ||
      outbox_state.slots == NULL || outbox_state.categories == NULL ||
      outbox_state.payloads == NULL ||
      outbox_state.writer_category_snapshot == NULL || outbox_state.writer_payload_snapshot == NULL ||
      outbox_state.writer_line_buffer == NULL) {
    release_storage_locked(&outbox_state);
    pthread_mutex_unlock(&outbox_state.mutex);
    return OUTBOX_STATUS_INTERNAL_ERROR;
  }

  for (uint32_t index = 0u; index < queue_capacity; ++index) {
    atomic_store_explicit(&outbox_state.slots[index].state_sequence,
                          (uint64_t)index,
                          memory_order_relaxed);
  }

  outbox_state.current_file_size_bytes =
      outbox_spool_file_size_bytes(outbox_state.active_file_path);
  if (config->default_provider_id != NULL && config->default_provider_id[0] != '\0' &&
      outbox_spool_get_provider_cursor_locked(&outbox_state,
                                                            config->default_provider_id,
                                                            1) == NULL) {
    release_storage_locked(&outbox_state);
    pthread_mutex_unlock(&outbox_state.mutex);
    return OUTBOX_STATUS_INVALID_ARGUMENT;
  }

  pthread_result = pthread_create(&outbox_state.writer_thread,
                                  NULL,
                                  outbox_writer_main,
                                  &outbox_state);
  if (pthread_result != 0) {
    release_storage_locked(&outbox_state);
    atomic_store_explicit(&outbox_state.started, 0u, memory_order_release);
    pthread_mutex_unlock(&outbox_state.mutex);
    return OUTBOX_STATUS_INTERNAL_ERROR;
  }
  outbox_state.writer_thread_started = 1u;
  atomic_store_explicit(&outbox_state.started, 1u, memory_order_release);
  pthread_mutex_unlock(&outbox_state.mutex);
  return OUTBOX_STATUS_OK;
}

outbox_status_t outbox_log(
    int32_t level,
    const char* category,
    const char* payload) {
  uint32_t category_length = 0u;
  uint32_t payload_length = 0u;
  uint64_t wall_time_unix_ms = 0u;

  if (category == NULL || payload == NULL || payload[0] == '\0') {
    if (atomic_load_explicit(&outbox_state.started, memory_order_acquire) != 0u &&
        atomic_load_explicit(&outbox_state.stop_requested, memory_order_acquire) == 0u) {
      atomic_fetch_add_explicit(&outbox_state.dropped_invalid_count, 1u, memory_order_relaxed);
    }
    notify_doorbell(OUTBOX_DOORBELL_DROPPED_RECORD);
    return OUTBOX_STATUS_INVALID_ARGUMENT;
  }

  wall_time_unix_ms = now_wall_time_unix_ms();

  if (!outbox_queue_try_enter_producer(&outbox_state)) {
    return OUTBOX_STATUS_BAD_STATE;
  }

  payload_length = bounded_strlen(payload, outbox_state.max_record_bytes);
  if (payload_length == outbox_state.max_record_bytes) {
    atomic_fetch_add_explicit(&outbox_state.dropped_record_too_large_count,
                              1u,
                              memory_order_relaxed);
    outbox_queue_leave_producer(&outbox_state);
    notify_doorbell(OUTBOX_DOORBELL_DROPPED_RECORD);
    return OUTBOX_STATUS_RECORD_TOO_LARGE;
  }

  category_length = bounded_strlen(category, OUTBOX_CATEGORY_CAPACITY - 1u);
  if (!outbox_queue_try_enqueue_record(&outbox_state,
                                                     level,
                                                     category,
                                                     category_length,
                                                     payload,
                                                     payload_length,
                                                     wall_time_unix_ms)) {
    atomic_fetch_add_explicit(&outbox_state.dropped_queue_full_count, 1u, memory_order_relaxed);
    outbox_queue_leave_producer(&outbox_state);
    notify_doorbell(OUTBOX_DOORBELL_DROPPED_RECORD);
    return OUTBOX_STATUS_QUEUE_FULL;
  }

  atomic_fetch_add_explicit(&outbox_state.accepted_count, 1u, memory_order_relaxed);
  outbox_queue_leave_producer(&outbox_state);
  outbox_queue_signal_not_empty(&outbox_state);
  return OUTBOX_STATUS_OK;
}

outbox_status_t outbox_flush(void) {
  if (atomic_load_explicit(&outbox_state.started, memory_order_acquire) == 0u) {
    return OUTBOX_STATUS_BAD_STATE;
  }
  pthread_mutex_lock(&outbox_state.wake_mutex);
  while (outbox_queue_depth(&outbox_state) > 0u ||
         atomic_load_explicit(&outbox_state.writer_in_flight, memory_order_acquire) != 0u) {
    pthread_cond_wait(&outbox_state.drained, &outbox_state.wake_mutex);
  }
  pthread_mutex_unlock(&outbox_state.wake_mutex);
  return OUTBOX_STATUS_OK;
}

void outbox_stop(void) {
  /* Stop is lifecycle control, not a hot path. Serialize the full shutdown so
   * direct C callers or test tooling cannot double-join the writer thread while
   * Android/Kotlin remains free to call stop idempotently. */
  pthread_mutex_lock(&outbox_stop_mutex);
  pthread_mutex_lock(&outbox_state.mutex);
  if (atomic_load_explicit(&outbox_state.started, memory_order_acquire) == 0u) {
    pthread_mutex_unlock(&outbox_state.mutex);
    pthread_mutex_unlock(&outbox_stop_mutex);
    return;
  }
  atomic_store_explicit(&outbox_state.stop_requested, 1u, memory_order_release);
  pthread_mutex_unlock(&outbox_state.mutex);

  pthread_mutex_lock(&outbox_state.wake_mutex);
  while (atomic_load_explicit(&outbox_state.active_producer_count, memory_order_acquire) != 0u) {
    pthread_cond_wait(&outbox_state.producers_drained, &outbox_state.wake_mutex);
  }
  pthread_cond_signal(&outbox_state.not_empty);
  pthread_mutex_unlock(&outbox_state.wake_mutex);

  if (outbox_state.writer_thread_started != 0u) {
    pthread_join(outbox_state.writer_thread, NULL);
  }

  pthread_mutex_lock(&outbox_state.mutex);
  release_storage_locked(&outbox_state);
  atomic_store_explicit(&outbox_state.started, 0u, memory_order_release);
  outbox_state.writer_thread_started = 0u;
  pthread_mutex_unlock(&outbox_state.mutex);
  pthread_mutex_unlock(&outbox_stop_mutex);
}

void outbox_get_stats(outbox_stats_t* out_stats) {
  if (out_stats == NULL) {
    return;
  }
  memset(out_stats, 0, sizeof(*out_stats));
  pthread_mutex_lock(&outbox_state.mutex);
  out_stats->started = atomic_load_explicit(&outbox_state.started, memory_order_acquire);
  out_stats->queue_capacity = outbox_state.queue_capacity;
  out_stats->queue_depth =
      out_stats->started == 0u ? 0u : outbox_queue_depth(&outbox_state);
  out_stats->queue_high_watermark =
      atomic_load_explicit(&outbox_state.queue_high_watermark, memory_order_relaxed);
  out_stats->next_sequence = atomic_load_explicit(&outbox_state.next_sequence, memory_order_relaxed);
  out_stats->accepted_count = atomic_load_explicit(&outbox_state.accepted_count, memory_order_relaxed);
  out_stats->written_count = atomic_load_explicit(&outbox_state.written_count, memory_order_relaxed);
  out_stats->dropped_queue_full_count =
      atomic_load_explicit(&outbox_state.dropped_queue_full_count, memory_order_relaxed);
  out_stats->dropped_invalid_count =
      atomic_load_explicit(&outbox_state.dropped_invalid_count, memory_order_relaxed);
  out_stats->dropped_record_too_large_count =
      atomic_load_explicit(&outbox_state.dropped_record_too_large_count, memory_order_relaxed);
  out_stats->write_failure_count =
      atomic_load_explicit(&outbox_state.write_failure_count, memory_order_relaxed);
  out_stats->current_file_size_bytes = outbox_state.current_file_size_bytes;
  out_stats->roll_count = outbox_state.roll_count;
  pthread_mutex_unlock(&outbox_state.mutex);
}
