#ifndef OBSERVABILITY_LOGGER_INTERNAL_H
#define OBSERVABILITY_LOGGER_INTERNAL_H

#include "observability_logger_core.h"

#include <pthread.h>
#include <stddef.h>
#include <stdatomic.h>
#include <stdint.h>

#define OBS_LOGGER_CATEGORY_CAPACITY 96u
#define OBS_LOGGER_CATEGORY_SLOT_BYTES 128u
#define OBS_LOGGER_DEFAULT_QUEUE_CAPACITY 256u
#define OBS_LOGGER_DEFAULT_RECORD_BYTES 4096u
#define OBS_LOGGER_DEFAULT_SEGMENT_SIZE_BYTES (512u * 1024u)
/* The fd protocol must not encode business assumptions about payload size.
 * Keep the frame ceiling at the Kotlin ByteArray boundary, then let memory,
 * disk, and caller-provided logger config decide whether a specific payload is
 * acceptable. */
#define OBS_LOGGER_MAX_PIPE_FRAME_BYTES 0x7fffffffu
#define OBS_LOGGER_SEGMENT_NAME_CAPACITY 33u
#define OBS_LOGGER_SEGMENT_PREFIX "segment-"
#define OBS_LOGGER_SEGMENT_SUFFIX ".log"
#define OBS_LOGGER_CURSOR_TOKEN_CAPACITY 48u
#define OBS_LOGGER_CURSOR_DIRECTORY_NAME "cursors"
#define OBS_LOGGER_PROVIDER_ID_CAPACITY 64u
#define OBS_LOGGER_MAX_PROVIDER_CURSORS 8u
#define OBS_LOGGER_STATS_FIELD_COUNT 13u
#if defined(__powerpc64__) || defined(__ppc64__) || defined(__PPC64__)
#define OBS_LOGGER_CACHELINE 128u
#else
#define OBS_LOGGER_CACHELINE 64u
#endif

typedef struct observability_logger_slot_t {
  /* Per-slot sequence keeps the queue bounded without a producer-side mutex.
   * Producers publish a filled slot with release; the single writer consumes it
   * with acquire and then advances the sequence to mark the slot reusable. */
  _Alignas(OBS_LOGGER_CACHELINE) _Atomic uint64_t state_sequence;
  uint64_t sequence;
  uint64_t wall_time_unix_ms;
  int32_t level;
  uint32_t category_length;
  uint32_t payload_length;
} observability_logger_slot_t;

typedef struct observability_logger_provider_cursor_t {
  uint32_t active;
  char provider_id[OBS_LOGGER_PROVIDER_ID_CAPACITY];
  char* cursor_file_path;
  uint64_t ack_segment_id;
  uint64_t ack_offset;
} observability_logger_provider_cursor_t;

/* Global runtime state.
 *
 * Thread ownership:
 *   - `mutex` protects file descriptors, segment paths, cursor state, and
 *     retention-sensitive file metadata.
 *   - `wake_mutex`/conditions coordinate the writer with producers and flush.
 *   - queue cursors, counters, and slot states are atomics because producers can
 *     be concurrent while the writer is single-consumer.
 *
 * File descriptor ownership:
 *   - Native keeps the read end of the command pipe and write ends of doorbell
 *     and response pipes.
 *   - Kotlin receives dup'd opposite ends and must close them on shutdown.
 */
typedef struct observability_logger_t {
  pthread_mutex_t mutex;
  pthread_mutex_t wake_mutex;
  pthread_cond_t not_empty;
  pthread_cond_t drained;
  pthread_cond_t producers_drained;
  pthread_t writer_thread;
  pthread_t control_thread;
  uint32_t writer_thread_started;
  uint32_t control_thread_started;
  uint32_t control_running;
  _Atomic uint32_t started;
  _Atomic uint32_t stop_requested;
  _Atomic uint32_t writer_in_flight;
  int32_t command_read_fd;
  int32_t doorbell_write_fd;
  int32_t record_write_fd;
  uint32_t data_available_doorbell_pending;
  int32_t active_fd;
  uint32_t queue_capacity;
  uint32_t max_record_bytes;
  uint64_t max_segment_size_bytes;
  uint32_t max_archived_segments;
  char* spool_directory_path;
  char* active_file_path;
  char* cursor_directory_path;
  uint64_t active_segment_id;
  uint32_t provider_cursor_count;
  observability_logger_provider_cursor_t provider_cursors[OBS_LOGGER_MAX_PROVIDER_CURSORS];
  observability_logger_slot_t* slots;
  char* categories;
  char* payloads;
  char* writer_category_snapshot;
  char* writer_payload_snapshot;
  char* writer_line_buffer;
  size_t writer_line_buffer_capacity;
  size_t payload_slot_bytes;
  _Alignas(OBS_LOGGER_CACHELINE) _Atomic uint64_t enqueue_cursor;
  _Alignas(OBS_LOGGER_CACHELINE) _Atomic uint64_t dequeue_cursor;
  _Alignas(OBS_LOGGER_CACHELINE) _Atomic uint32_t active_producer_count;
  _Atomic uint64_t next_sequence;
  _Atomic uint32_t queue_high_watermark;
  _Atomic uint64_t accepted_count;
  _Atomic uint64_t written_count;
  _Atomic uint64_t dropped_queue_full_count;
  _Atomic uint64_t dropped_invalid_count;
  _Atomic uint64_t dropped_record_too_large_count;
  _Atomic uint64_t write_failure_count;
  uint64_t current_file_size_bytes;
  uint64_t roll_count;
} observability_logger_t;

extern observability_logger_t obs_logger_state;

void observability_logger_core_notify_data_available_once(void);

#endif
