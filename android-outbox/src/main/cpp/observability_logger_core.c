#include "observability_logger_core.h"
#include "observability_logger_control.h"
#include "observability_logger_frame.h"
#include "observability_logger_internal.h"
#include "observability_logger_queue.h"
#include "observability_logger_spool.h"
#include "observability_logger_writer.h"

#include <errno.h>
#include <pthread.h>
#include <stddef.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define OBS_LOGGER_DOORBELL_HANDSHAKE 0u
#define OBS_LOGGER_DOORBELL_DATA_AVAILABLE 1u
#define OBS_LOGGER_DOORBELL_DROPPED_RECORD 2u
#define OBS_LOGGER_DOORBELL_PAYLOAD_BYTES 4u
#define OBS_LOGGER_DOORBELL_FRAME_BYTES \
  (OBS_LOGGER_FRAME_HEADER_BYTES + OBS_LOGGER_DOORBELL_PAYLOAD_BYTES)

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

observability_logger_t obs_logger_state = {
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

static pthread_mutex_t obs_logger_stop_mutex = PTHREAD_MUTEX_INITIALIZER;

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
  if (size == 0u || posix_memalign(&data, OBS_LOGGER_CACHELINE, size) != 0) {
    return NULL;
  }
  memset(data, 0, size);
  return data;
}

static int write_doorbell_frame_to_fd(int fd, uint32_t event) {
  uint8_t frame[OBS_LOGGER_DOORBELL_FRAME_BYTES] = {};
  observability_logger_write_u32_be(OBS_LOGGER_DOORBELL_PAYLOAD_BYTES, frame);
  observability_logger_write_u32_le(event, frame + OBS_LOGGER_FRAME_HEADER_BYTES);

  for (uint32_t attempt = 0u; attempt < 2u; attempt++) {
    if (observability_logger_write_pipe_all_bytes(fd, frame, sizeof(frame))) {
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
  /* Keep the fd write under the logger mutex so stop/close cannot race with a
   * reused fd. The frame is fixed at 8 bytes and the pipe is nonblocking, so this
   * critical section stays bounded on the log writer path. */
  pthread_mutex_lock(&obs_logger_state.mutex);
  if (obs_logger_state.doorbell_write_fd >= 0) {
    write_doorbell_frame_to_fd(obs_logger_state.doorbell_write_fd, event);
  }
  pthread_mutex_unlock(&obs_logger_state.mutex);
}

void observability_logger_core_notify_data_available_once(void) {
  pthread_mutex_lock(&obs_logger_state.mutex);
  /* DATA_AVAILABLE is a doorbell, not a counter. Keep only one pending wakeup
   * until Kotlin asks for a batch; this avoids flooding the pipe during bursts
   * while still preserving durable records in segment files. */
  if (obs_logger_state.doorbell_write_fd >= 0 &&
      obs_logger_state.data_available_doorbell_pending == 0u &&
      write_doorbell_frame_to_fd(obs_logger_state.doorbell_write_fd,
                                 OBS_LOGGER_DOORBELL_DATA_AVAILABLE)) {
    obs_logger_state.data_available_doorbell_pending = 1u;
  }
  pthread_mutex_unlock(&obs_logger_state.mutex);
}

static void release_storage_locked(observability_logger_t* logger) {
  if (logger->active_fd >= 0) {
    close(logger->active_fd);
  }
  free(logger->spool_directory_path);
  free(logger->active_file_path);
  free(logger->cursor_directory_path);
  for (uint32_t index = 0u; index < logger->provider_cursor_count; ++index) {
    free(logger->provider_cursors[index].cursor_file_path);
  }
  free(logger->slots);
  free(logger->categories);
  free(logger->payloads);
  free(logger->writer_category_snapshot);
  free(logger->writer_payload_snapshot);
  free(logger->writer_line_buffer);
  logger->spool_directory_path = NULL;
  logger->active_file_path = NULL;
  logger->cursor_directory_path = NULL;
  logger->active_fd = -1;
  logger->active_segment_id = 0u;
  logger->provider_cursor_count = 0u;
  memset(logger->provider_cursors, 0, sizeof(logger->provider_cursors));
  logger->slots = NULL;
  logger->categories = NULL;
  logger->payloads = NULL;
  logger->writer_category_snapshot = NULL;
  logger->writer_payload_snapshot = NULL;
  logger->writer_line_buffer = NULL;
  logger->writer_line_buffer_capacity = 0u;
  logger->payload_slot_bytes = 0u;
}

observability_logger_status_t observability_logger_open_pipes(
    observability_logger_pipes_t* out_pipes) {
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
   * The command read fd and native write fds stay in obs_logger_state. Kotlin receives
   * duplicated peer fds, making close ownership explicit on both sides. */
  if (out_pipes == NULL) {
    return OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT;
  }
  out_pipes->command_write_fd = -1;
  out_pipes->doorbell_read_fd = -1;
  out_pipes->record_read_fd = -1;

  pthread_mutex_lock(&obs_logger_state.mutex);
  if (obs_logger_state.control_thread_started != 0u || obs_logger_state.command_read_fd != -1 ||
      obs_logger_state.doorbell_write_fd != -1 || obs_logger_state.record_write_fd != -1) {
    pthread_mutex_unlock(&obs_logger_state.mutex);
    return OBSERVABILITY_LOGGER_STATUS_BAD_STATE;
  }
  pthread_mutex_unlock(&obs_logger_state.mutex);

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
    return OBSERVABILITY_LOGGER_STATUS_INTERNAL_ERROR;
  }
  if (!observability_logger_set_fd_cloexec(command_pipe[0]) ||
      !observability_logger_set_fd_cloexec(command_pipe[1]) ||
      !observability_logger_set_fd_cloexec(doorbell_pipe[0]) ||
      !observability_logger_set_fd_cloexec(doorbell_pipe[1]) ||
      !observability_logger_set_fd_cloexec(record_pipe[0]) ||
      !observability_logger_set_fd_cloexec(record_pipe[1]) ||
      !observability_logger_set_fd_nonblocking(command_pipe[0]) ||
      !observability_logger_set_fd_nonblocking(doorbell_pipe[1])) {
    close(command_pipe[0]);
    close(command_pipe[1]);
    close(doorbell_pipe[0]);
    close(doorbell_pipe[1]);
    close(record_pipe[0]);
    close(record_pipe[1]);
    return OBSERVABILITY_LOGGER_STATUS_INTERNAL_ERROR;
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
    return OBSERVABILITY_LOGGER_STATUS_INTERNAL_ERROR;
  }
  if (!observability_logger_set_fd_cloexec(kotlin_write_fd) ||
      !observability_logger_set_fd_cloexec(kotlin_read_fd) ||
      !observability_logger_set_fd_cloexec(kotlin_record_read_fd)) {
    close(command_pipe[0]);
    close(doorbell_pipe[1]);
    close(record_pipe[1]);
    close(kotlin_write_fd);
    close(kotlin_read_fd);
    close(kotlin_record_read_fd);
    return OBSERVABILITY_LOGGER_STATUS_INTERNAL_ERROR;
  }

  pthread_mutex_lock(&obs_logger_state.mutex);
  obs_logger_state.command_read_fd = command_pipe[0];
  obs_logger_state.doorbell_write_fd = doorbell_pipe[1];
  obs_logger_state.record_write_fd = record_pipe[1];
  obs_logger_state.data_available_doorbell_pending = 0u;
  obs_logger_state.control_running = 1u;
  obs_logger_state.control_thread_started = 0u;
  pthread_result = pthread_create(&obs_logger_state.control_thread,
                                  NULL,
                                  observability_logger_control_main,
                                  &obs_logger_state);
  if (pthread_result != 0) {
    obs_logger_state.command_read_fd = -1;
    obs_logger_state.doorbell_write_fd = -1;
    obs_logger_state.record_write_fd = -1;
    obs_logger_state.control_running = 0u;
    pthread_mutex_unlock(&obs_logger_state.mutex);
    close(command_pipe[0]);
    close(doorbell_pipe[1]);
    close(record_pipe[1]);
    close(kotlin_write_fd);
    close(kotlin_read_fd);
    close(kotlin_record_read_fd);
    return OBSERVABILITY_LOGGER_STATUS_INTERNAL_ERROR;
  }
  obs_logger_state.control_thread_started = 1u;
  pthread_mutex_unlock(&obs_logger_state.mutex);

  out_pipes->command_write_fd = kotlin_write_fd;
  out_pipes->doorbell_read_fd = kotlin_read_fd;
  out_pipes->record_read_fd = kotlin_record_read_fd;
  notify_doorbell(OBS_LOGGER_DOORBELL_HANDSHAKE);
  observability_logger_control_notify_if_unacked_records_available();
  return OBSERVABILITY_LOGGER_STATUS_OK;
}

void observability_logger_close_pipes(void) {
  pthread_t control_thread;
  uint32_t should_join = 0u;
  int command_read_fd = -1;
  int doorbell_write_fd = -1;
  int record_write_fd = -1;

  pthread_mutex_lock(&obs_logger_state.mutex);
  obs_logger_state.control_running = 0u;
  command_read_fd = obs_logger_state.command_read_fd;
  doorbell_write_fd = obs_logger_state.doorbell_write_fd;
  record_write_fd = obs_logger_state.record_write_fd;
  obs_logger_state.command_read_fd = -1;
  obs_logger_state.doorbell_write_fd = -1;
  obs_logger_state.record_write_fd = -1;
  obs_logger_state.data_available_doorbell_pending = 0u;
  should_join = obs_logger_state.control_thread_started;
  control_thread = obs_logger_state.control_thread;
  pthread_mutex_unlock(&obs_logger_state.mutex);

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

  pthread_mutex_lock(&obs_logger_state.mutex);
  obs_logger_state.control_thread_started = 0u;
  pthread_mutex_unlock(&obs_logger_state.mutex);
}

observability_logger_status_t observability_logger_start(
    const observability_logger_config_t* config) {
  uint32_t queue_capacity = OBS_LOGGER_DEFAULT_QUEUE_CAPACITY;
  uint32_t max_record_bytes = OBS_LOGGER_DEFAULT_RECORD_BYTES;
  uint64_t max_segment_size_bytes = OBS_LOGGER_DEFAULT_SEGMENT_SIZE_BYTES;
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
    return OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT;
  }

  pthread_mutex_lock(&obs_logger_state.mutex);
  if (atomic_load_explicit(&obs_logger_state.started, memory_order_acquire) != 0u) {
    pthread_mutex_unlock(&obs_logger_state.mutex);
    return OBSERVABILITY_LOGGER_STATUS_BAD_STATE;
  }

  queue_capacity = config->queue_capacity == 0u ? queue_capacity : config->queue_capacity;
  max_record_bytes = config->max_record_bytes == 0u ? max_record_bytes : config->max_record_bytes;
  max_segment_size_bytes = config->max_segment_size_bytes == 0u ? max_segment_size_bytes
                                                                : config->max_segment_size_bytes;
  max_archived_segments = config->max_archived_segments;
  if (queue_capacity == 0u ||
      max_record_bytes == 0u ||
      max_segment_size_bytes == 0u) {
    pthread_mutex_unlock(&obs_logger_state.mutex);
    return OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT;
  }
  if ((size_t)queue_capacity > SIZE_MAX / sizeof(*obs_logger_state.slots) ||
      (size_t)queue_capacity > SIZE_MAX / OBS_LOGGER_CATEGORY_SLOT_BYTES) {
    pthread_mutex_unlock(&obs_logger_state.mutex);
    return OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT;
  }

  atomic_store_explicit(&obs_logger_state.stop_requested, 0u, memory_order_release);
  atomic_store_explicit(&obs_logger_state.writer_in_flight, 0u, memory_order_release);
  obs_logger_state.writer_thread_started = 0u;
  obs_logger_state.queue_capacity = queue_capacity;
  obs_logger_state.max_record_bytes = max_record_bytes;
  obs_logger_state.max_segment_size_bytes = max_segment_size_bytes;
  obs_logger_state.max_archived_segments = max_archived_segments;
  atomic_store_explicit(&obs_logger_state.enqueue_cursor, 0u, memory_order_release);
  atomic_store_explicit(&obs_logger_state.dequeue_cursor, 0u, memory_order_release);
  atomic_store_explicit(&obs_logger_state.active_producer_count, 0u, memory_order_release);
  atomic_store_explicit(&obs_logger_state.next_sequence, 0u, memory_order_release);
  atomic_store_explicit(&obs_logger_state.queue_high_watermark, 0u, memory_order_release);
  atomic_store_explicit(&obs_logger_state.accepted_count, 0u, memory_order_release);
  atomic_store_explicit(&obs_logger_state.written_count, 0u, memory_order_release);
  atomic_store_explicit(&obs_logger_state.dropped_queue_full_count, 0u, memory_order_release);
  atomic_store_explicit(&obs_logger_state.dropped_invalid_count, 0u, memory_order_release);
  atomic_store_explicit(&obs_logger_state.dropped_record_too_large_count, 0u, memory_order_release);
  atomic_store_explicit(&obs_logger_state.write_failure_count, 0u, memory_order_release);
  obs_logger_state.data_available_doorbell_pending = 0u;
  obs_logger_state.current_file_size_bytes = 0u;
  obs_logger_state.roll_count = 0u;
  obs_logger_state.active_fd = -1;
  obs_logger_state.writer_line_buffer_capacity =
      observability_logger_spool_line_buffer_capacity(max_record_bytes);
  obs_logger_state.payload_slot_bytes = align_up_size((size_t)max_record_bytes, OBS_LOGGER_CACHELINE);
  if (obs_logger_state.payload_slot_bytes == 0u ||
      (size_t)queue_capacity > SIZE_MAX / obs_logger_state.payload_slot_bytes) {
    pthread_mutex_unlock(&obs_logger_state.mutex);
    return OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT;
  }
  if (!observability_logger_spool_ensure_directory(config->spool_directory_path)) {
    pthread_mutex_unlock(&obs_logger_state.mutex);
    return OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT;
  }

  obs_logger_state.spool_directory_path = strdup(config->spool_directory_path);
  obs_logger_state.cursor_directory_path =
      observability_logger_spool_cursor_directory_path(config->spool_directory_path);
  if (obs_logger_state.cursor_directory_path != NULL &&
      !observability_logger_spool_ensure_directory(obs_logger_state.cursor_directory_path)) {
    release_storage_locked(&obs_logger_state);
    pthread_mutex_unlock(&obs_logger_state.mutex);
    return OBSERVABILITY_LOGGER_STATUS_INTERNAL_ERROR;
  }
  if (!observability_logger_spool_scan_segment_bounds(config->spool_directory_path,
                           &min_segment_id,
                           &max_segment_id,
                           &segment_count)) {
    release_storage_locked(&obs_logger_state);
    pthread_mutex_unlock(&obs_logger_state.mutex);
    return OBSERVABILITY_LOGGER_STATUS_INTERNAL_ERROR;
  }
  obs_logger_state.active_segment_id = segment_count == 0u ? 1u : max_segment_id;
  obs_logger_state.active_file_path =
      observability_logger_spool_segment_path(config->spool_directory_path,
                                              obs_logger_state.active_segment_id);
  obs_logger_state.active_fd =
      observability_logger_spool_open_segment_append_fd(obs_logger_state.active_file_path);
  obs_logger_state.slots =
      (observability_logger_slot_t*)zalloc_cacheline_aligned(
          (size_t)queue_capacity * sizeof(*obs_logger_state.slots));
  obs_logger_state.categories =
      (char*)zalloc_cacheline_aligned((size_t)queue_capacity * OBS_LOGGER_CATEGORY_SLOT_BYTES);
  obs_logger_state.payloads =
      (char*)zalloc_cacheline_aligned((size_t)queue_capacity * obs_logger_state.payload_slot_bytes);
  obs_logger_state.writer_category_snapshot = (char*)calloc(1u, OBS_LOGGER_CATEGORY_CAPACITY);
  obs_logger_state.writer_payload_snapshot = (char*)calloc(1u, (size_t)max_record_bytes);
  obs_logger_state.writer_line_buffer = (char*)calloc(1u, obs_logger_state.writer_line_buffer_capacity);
  if (obs_logger_state.spool_directory_path == NULL || obs_logger_state.active_file_path == NULL ||
      obs_logger_state.cursor_directory_path == NULL || obs_logger_state.active_fd < 0 ||
      obs_logger_state.slots == NULL || obs_logger_state.categories == NULL ||
      obs_logger_state.payloads == NULL ||
      obs_logger_state.writer_category_snapshot == NULL || obs_logger_state.writer_payload_snapshot == NULL ||
      obs_logger_state.writer_line_buffer == NULL) {
    release_storage_locked(&obs_logger_state);
    pthread_mutex_unlock(&obs_logger_state.mutex);
    return OBSERVABILITY_LOGGER_STATUS_INTERNAL_ERROR;
  }

  for (uint32_t index = 0u; index < queue_capacity; ++index) {
    atomic_store_explicit(&obs_logger_state.slots[index].state_sequence,
                          (uint64_t)index,
                          memory_order_relaxed);
  }

  obs_logger_state.current_file_size_bytes =
      observability_logger_spool_file_size_bytes(obs_logger_state.active_file_path);
  if (config->default_provider_id != NULL && config->default_provider_id[0] != '\0' &&
      observability_logger_spool_get_provider_cursor_locked(&obs_logger_state,
                                                            config->default_provider_id,
                                                            1) == NULL) {
    release_storage_locked(&obs_logger_state);
    pthread_mutex_unlock(&obs_logger_state.mutex);
    return OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT;
  }

  pthread_result = pthread_create(&obs_logger_state.writer_thread,
                                  NULL,
                                  observability_logger_writer_main,
                                  &obs_logger_state);
  if (pthread_result != 0) {
    release_storage_locked(&obs_logger_state);
    atomic_store_explicit(&obs_logger_state.started, 0u, memory_order_release);
    pthread_mutex_unlock(&obs_logger_state.mutex);
    return OBSERVABILITY_LOGGER_STATUS_INTERNAL_ERROR;
  }
  obs_logger_state.writer_thread_started = 1u;
  atomic_store_explicit(&obs_logger_state.started, 1u, memory_order_release);
  pthread_mutex_unlock(&obs_logger_state.mutex);
  return OBSERVABILITY_LOGGER_STATUS_OK;
}

observability_logger_status_t observability_logger_log(
    int32_t level,
    const char* category,
    const char* payload) {
  uint32_t category_length = 0u;
  uint32_t payload_length = 0u;
  uint64_t wall_time_unix_ms = 0u;

  if (category == NULL || payload == NULL || payload[0] == '\0') {
    if (atomic_load_explicit(&obs_logger_state.started, memory_order_acquire) != 0u &&
        atomic_load_explicit(&obs_logger_state.stop_requested, memory_order_acquire) == 0u) {
      atomic_fetch_add_explicit(&obs_logger_state.dropped_invalid_count, 1u, memory_order_relaxed);
    }
    notify_doorbell(OBS_LOGGER_DOORBELL_DROPPED_RECORD);
    return OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT;
  }

  wall_time_unix_ms = now_wall_time_unix_ms();

  if (!observability_logger_queue_try_enter_producer(&obs_logger_state)) {
    return OBSERVABILITY_LOGGER_STATUS_BAD_STATE;
  }

  payload_length = bounded_strlen(payload, obs_logger_state.max_record_bytes);
  if (payload_length == obs_logger_state.max_record_bytes) {
    atomic_fetch_add_explicit(&obs_logger_state.dropped_record_too_large_count,
                              1u,
                              memory_order_relaxed);
    observability_logger_queue_leave_producer(&obs_logger_state);
    notify_doorbell(OBS_LOGGER_DOORBELL_DROPPED_RECORD);
    return OBSERVABILITY_LOGGER_STATUS_RECORD_TOO_LARGE;
  }

  category_length = bounded_strlen(category, OBS_LOGGER_CATEGORY_CAPACITY - 1u);
  if (!observability_logger_queue_try_enqueue_record(&obs_logger_state,
                                                     level,
                                                     category,
                                                     category_length,
                                                     payload,
                                                     payload_length,
                                                     wall_time_unix_ms)) {
    atomic_fetch_add_explicit(&obs_logger_state.dropped_queue_full_count, 1u, memory_order_relaxed);
    observability_logger_queue_leave_producer(&obs_logger_state);
    notify_doorbell(OBS_LOGGER_DOORBELL_DROPPED_RECORD);
    return OBSERVABILITY_LOGGER_STATUS_QUEUE_FULL;
  }

  atomic_fetch_add_explicit(&obs_logger_state.accepted_count, 1u, memory_order_relaxed);
  observability_logger_queue_leave_producer(&obs_logger_state);
  observability_logger_queue_signal_not_empty(&obs_logger_state);
  return OBSERVABILITY_LOGGER_STATUS_OK;
}

observability_logger_status_t observability_logger_flush(void) {
  if (atomic_load_explicit(&obs_logger_state.started, memory_order_acquire) == 0u) {
    return OBSERVABILITY_LOGGER_STATUS_BAD_STATE;
  }
  pthread_mutex_lock(&obs_logger_state.wake_mutex);
  while (observability_logger_queue_depth(&obs_logger_state) > 0u ||
         atomic_load_explicit(&obs_logger_state.writer_in_flight, memory_order_acquire) != 0u) {
    pthread_cond_wait(&obs_logger_state.drained, &obs_logger_state.wake_mutex);
  }
  pthread_mutex_unlock(&obs_logger_state.wake_mutex);
  return OBSERVABILITY_LOGGER_STATUS_OK;
}

void observability_logger_stop(void) {
  /* Stop is lifecycle control, not a hot path. Serialize the full shutdown so
   * direct C callers or test tooling cannot double-join the writer thread while
   * Android/Kotlin remains free to call stop idempotently. */
  pthread_mutex_lock(&obs_logger_stop_mutex);
  pthread_mutex_lock(&obs_logger_state.mutex);
  if (atomic_load_explicit(&obs_logger_state.started, memory_order_acquire) == 0u) {
    pthread_mutex_unlock(&obs_logger_state.mutex);
    pthread_mutex_unlock(&obs_logger_stop_mutex);
    return;
  }
  atomic_store_explicit(&obs_logger_state.stop_requested, 1u, memory_order_release);
  pthread_mutex_unlock(&obs_logger_state.mutex);

  pthread_mutex_lock(&obs_logger_state.wake_mutex);
  while (atomic_load_explicit(&obs_logger_state.active_producer_count, memory_order_acquire) != 0u) {
    pthread_cond_wait(&obs_logger_state.producers_drained, &obs_logger_state.wake_mutex);
  }
  pthread_cond_signal(&obs_logger_state.not_empty);
  pthread_mutex_unlock(&obs_logger_state.wake_mutex);

  if (obs_logger_state.writer_thread_started != 0u) {
    pthread_join(obs_logger_state.writer_thread, NULL);
  }

  pthread_mutex_lock(&obs_logger_state.mutex);
  release_storage_locked(&obs_logger_state);
  atomic_store_explicit(&obs_logger_state.started, 0u, memory_order_release);
  obs_logger_state.writer_thread_started = 0u;
  pthread_mutex_unlock(&obs_logger_state.mutex);
  pthread_mutex_unlock(&obs_logger_stop_mutex);
}

void observability_logger_get_stats(observability_logger_stats_t* out_stats) {
  if (out_stats == NULL) {
    return;
  }
  memset(out_stats, 0, sizeof(*out_stats));
  pthread_mutex_lock(&obs_logger_state.mutex);
  out_stats->started = atomic_load_explicit(&obs_logger_state.started, memory_order_acquire);
  out_stats->queue_capacity = obs_logger_state.queue_capacity;
  out_stats->queue_depth =
      out_stats->started == 0u ? 0u : observability_logger_queue_depth(&obs_logger_state);
  out_stats->queue_high_watermark =
      atomic_load_explicit(&obs_logger_state.queue_high_watermark, memory_order_relaxed);
  out_stats->next_sequence = atomic_load_explicit(&obs_logger_state.next_sequence, memory_order_relaxed);
  out_stats->accepted_count = atomic_load_explicit(&obs_logger_state.accepted_count, memory_order_relaxed);
  out_stats->written_count = atomic_load_explicit(&obs_logger_state.written_count, memory_order_relaxed);
  out_stats->dropped_queue_full_count =
      atomic_load_explicit(&obs_logger_state.dropped_queue_full_count, memory_order_relaxed);
  out_stats->dropped_invalid_count =
      atomic_load_explicit(&obs_logger_state.dropped_invalid_count, memory_order_relaxed);
  out_stats->dropped_record_too_large_count =
      atomic_load_explicit(&obs_logger_state.dropped_record_too_large_count, memory_order_relaxed);
  out_stats->write_failure_count =
      atomic_load_explicit(&obs_logger_state.write_failure_count, memory_order_relaxed);
  out_stats->current_file_size_bytes = obs_logger_state.current_file_size_bytes;
  out_stats->roll_count = obs_logger_state.roll_count;
  pthread_mutex_unlock(&obs_logger_state.mutex);
}
