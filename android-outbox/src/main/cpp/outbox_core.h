#ifndef OUTBOX_CORE_H
#define OUTBOX_CORE_H

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

typedef enum outbox_status_t {
  OUTBOX_STATUS_OK = 0,
  OUTBOX_STATUS_INVALID_ARGUMENT = 1,
  OUTBOX_STATUS_BAD_STATE = 2,
  OUTBOX_STATUS_QUEUE_FULL = 3,
  OUTBOX_STATUS_RECORD_TOO_LARGE = 4,
  OUTBOX_STATUS_INTERNAL_ERROR = 5
} outbox_status_t;

typedef struct outbox_config_t {
  const char* spool_directory_path;
  const char* default_provider_id;
  uint32_t queue_capacity;
  uint32_t max_record_bytes;
  uint64_t max_segment_size_bytes;
  uint32_t max_archived_segments;
} outbox_config_t;

typedef struct outbox_stats_t {
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
} outbox_stats_t;

typedef struct outbox_pipes_t {
  int32_t command_write_fd;
  int32_t doorbell_read_fd;
  int32_t record_read_fd;
} outbox_pipes_t;

typedef struct outbox_record_batch_t {
  char* ack_token;
  char** records;
  uint32_t record_count;
} outbox_record_batch_t;

/* Starts a process-local file-first outbox.
 * The native layer owns only bounded buffering, file persistence, rotation,
 * and counters. Payload classification, PII scrubbing, and remote upload stay
 * in Kotlin.
 *
 * `spool_directory_path` is a native-owned directory. The outbox creates and
 * manages files inside that directory so Kotlin does not depend on active file
 * names, segment naming, or future cursor state layout. */
outbox_status_t outbox_start(
    const outbox_config_t* config);
/* Opens Kotlin-owned pipe FDs.
 *
 * Kotlin writes opaque command frames to `command_write_fd`. Native writes
 * compact length-prefixed doorbell frames to `doorbell_read_fd` so Kotlin can
 * wake a consumer without polling spool files. Batch reads are requested as
 * command frames and returned as one response frame on `record_read_fd`.
 * Payload records still live in native-managed files; doorbells are only
 * notifications. */
outbox_status_t outbox_open_pipes(
    outbox_pipes_t* out_pipes);
void outbox_close_pipes(void);
/* Enqueues one compact sanitized record. The payload is treated as opaque text;
 * this layer does not parse JSON or know about remote backends.
 *
 * File format:
 *   wall_time_ms<TAB>sequence<TAB>level<TAB>category<TAB>payload<LF>
 *
 * Category and payload delimiters/newlines are normalized to spaces before
 * writing. */
outbox_status_t outbox_log(
    int32_t level,
    const char* category,
    const char* payload);
outbox_status_t outbox_flush(void);
void outbox_stop(void);
void outbox_get_stats(outbox_stats_t* out_stats);
outbox_status_t outbox_read_next_batch(
    const char* provider_id,
    uint32_t max_records,
    uint32_t max_bytes,
    outbox_record_batch_t* out_batch);
/* Commits the provider cursor after Kotlin has handled the batch.
 *
 * The provider id is only a cursor namespace. Native returns raw durable lines
 * from the shared spool; Kotlin decides whether those lines are posted,
 * transformed, skipped, or retried. A failed remote transport should not call
 * ack, so the same provider cursor can read the batch again later. */
outbox_status_t outbox_ack(
    const char* provider_id,
    const char* ack_token);
void outbox_free_record_batch(
    outbox_record_batch_t* batch);

#ifdef __cplusplus
}
#endif

#endif
