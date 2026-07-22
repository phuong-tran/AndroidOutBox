#ifndef OBSERVABILITY_LOGGER_CORE_H
#define OBSERVABILITY_LOGGER_CORE_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* AndroidOutBox core: a tiny file-backed outbox for app-owned records.
 *
 * Mental model:
 *   - Producers call `log()` from app threads. The call only validates input,
 *     copies compact text into a bounded native MPSC queue, and returns.
 *   - A single native writer thread drains that queue into append-only segment
 *     files. Rotation and retention happen only on that writer path.
 *   - Kotlin owns delivery. It opens pipes, sends control commands, reads
 *     durable batches from the spool, and acks them after upload.
 *   - JNI is intentionally thin. It only returns pipe file descriptors; the
 *     command protocol stays byte-oriented so native does not construct Java
 *     object graphs or call back into the JVM on the hot path.
 *
 * Durability is best-effort and bounded. This component preserves recent
 * failure context without becoming unbounded audit storage. If the queue or
 * disk budget is exceeded, records can be dropped and counters/doorbells report
 * that pressure back to Kotlin.
 */

typedef enum observability_logger_status_t {
  OBSERVABILITY_LOGGER_STATUS_OK = 0,
  OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT = 1,
  OBSERVABILITY_LOGGER_STATUS_BAD_STATE = 2,
  OBSERVABILITY_LOGGER_STATUS_QUEUE_FULL = 3,
  OBSERVABILITY_LOGGER_STATUS_RECORD_TOO_LARGE = 4,
  OBSERVABILITY_LOGGER_STATUS_INTERNAL_ERROR = 5
} observability_logger_status_t;

typedef struct observability_logger_config_t {
  const char* spool_directory_path;
  const char* default_provider_id;
  uint32_t queue_capacity;
  uint32_t max_record_bytes;
  uint64_t max_segment_size_bytes;
  uint32_t max_archived_segments;
} observability_logger_config_t;

typedef struct observability_logger_stats_t {
  uint32_t started;
  uint32_t queue_capacity;
  uint32_t queue_depth;
  uint32_t queue_high_watermark;
  uint64_t next_sequence;
  uint64_t accepted_count;
  uint64_t written_count;
  uint64_t dropped_queue_full_count;
  uint64_t dropped_invalid_count;
  uint64_t dropped_record_too_large_count;
  uint64_t write_failure_count;
  uint64_t current_file_size_bytes;
  uint64_t roll_count;
} observability_logger_stats_t;

typedef struct observability_logger_pipes_t {
  int32_t command_write_fd;
  int32_t doorbell_read_fd;
  int32_t record_read_fd;
} observability_logger_pipes_t;

typedef struct observability_logger_record_batch_t {
  char* ack_token;
  char** records;
  uint32_t record_count;
} observability_logger_record_batch_t;

/* Starts a process-local file-first logger.
 * The native layer owns only bounded buffering, file persistence, rotation,
 * and counters. Payload classification, PII scrubbing, and remote upload stay
 * in Kotlin.
 *
 * `spool_directory_path` is a native-owned directory. The logger creates and
 * manages files inside that directory so Kotlin does not depend on active file
 * names, segment naming, or future cursor state layout. */
observability_logger_status_t observability_logger_start(
    const observability_logger_config_t* config);
/* Opens Kotlin-owned pipe FDs.
 *
 * Kotlin writes opaque command frames to `command_write_fd`. Native writes
 * compact length-prefixed doorbell frames to `doorbell_read_fd` so Kotlin can
 * wake a consumer without polling spool files. Batch reads are requested as
 * command frames and returned as one response frame on `record_read_fd`.
 * Payload records still live in native-managed files; doorbells are only
 * notifications. */
observability_logger_status_t observability_logger_open_pipes(
    observability_logger_pipes_t* out_pipes);
void observability_logger_close_pipes(void);
/* Enqueues one compact sanitized record. The payload is treated as opaque text;
 * this layer does not parse JSON or know about remote backends.
 *
 * File format:
 *   wall_time_ms<TAB>sequence<TAB>level<TAB>category<TAB>payload<LF>
 *
 * Category and payload delimiters/newlines are normalized to spaces before
 * writing. */
observability_logger_status_t observability_logger_log(
    int32_t level,
    const char* category,
    const char* payload);
observability_logger_status_t observability_logger_flush(void);
void observability_logger_stop(void);
void observability_logger_get_stats(observability_logger_stats_t* out_stats);
observability_logger_status_t observability_logger_read_next_batch(
    const char* provider_id,
    uint32_t max_records,
    uint32_t max_bytes,
    observability_logger_record_batch_t* out_batch);
/* Commits the provider cursor after Kotlin has handled the batch.
 *
 * The provider id is only a cursor namespace. Native returns raw durable lines
 * from the shared spool; Kotlin decides whether those lines are posted,
 * transformed, skipped, or retried. A failed remote transport should not call
 * ack, so the same provider cursor can read the batch again later. */
observability_logger_status_t observability_logger_ack(
    const char* provider_id,
    const char* ack_token);
void observability_logger_free_record_batch(
    observability_logger_record_batch_t* batch);

#ifdef __cplusplus
}
#endif

#endif
