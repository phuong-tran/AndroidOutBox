#include "observability_logger_spool.h"

#include "observability_logger_frame.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

uint64_t observability_logger_spool_file_size_bytes(const char* path) {
  struct stat stat_buffer;
  if (path == NULL || stat(path, &stat_buffer) != 0) {
    return 0u;
  }
  return stat_buffer.st_size < 0 ? 0u : (uint64_t)stat_buffer.st_size;
}

int observability_logger_spool_ensure_directory(const char* path) {
  struct stat stat_buffer;
  if (path == NULL || path[0] == '\0') {
    return 0;
  }
  if (stat(path, &stat_buffer) == 0) {
    return S_ISDIR(stat_buffer.st_mode) ? 1 : 0;
  }
  if (errno != ENOENT) {
    return 0;
  }
  return mkdir(path, 0700) == 0 ? 1 : 0;
}

static char* path_join(const char* directory, const char* file_name) {
  const char separator = '/';
  const size_t directory_length = strlen(directory);
  const int needs_separator =
      directory_length > 0u && directory[directory_length - 1u] == separator ? 0 : 1;
  const int required = snprintf(NULL,
                                0,
                                "%s%s%s",
                                directory,
                                needs_separator != 0 ? "/" : "",
                                file_name);
  char* path = NULL;
  if (required <= 0) {
    return NULL;
  }
  path = (char*)calloc(1u, (size_t)required + 1u);
  if (path == NULL) {
    return NULL;
  }
  snprintf(path,
           (size_t)required + 1u,
           "%s%s%s",
           directory,
           needs_separator != 0 ? "/" : "",
           file_name);
  return path;
}

static int segment_file_name(uint64_t segment_id, char* buffer, size_t buffer_capacity) {
  return snprintf(buffer,
                  buffer_capacity,
                  "%s%020llu%s",
                  OBS_LOGGER_SEGMENT_PREFIX,
                  (unsigned long long)segment_id,
                  OBS_LOGGER_SEGMENT_SUFFIX);
}

size_t observability_logger_spool_line_buffer_capacity(uint32_t max_record_bytes) {
  return (size_t)max_record_bytes + (size_t)OBS_LOGGER_CATEGORY_CAPACITY + 192u;
}

static int parse_segment_id(const char* file_name, uint64_t* out_segment_id) {
  unsigned long long segment_id = 0u;
  char trailing = '\0';
  if (file_name == NULL || out_segment_id == NULL) {
    return 0;
  }
  if (sscanf(file_name,
             OBS_LOGGER_SEGMENT_PREFIX "%llu" OBS_LOGGER_SEGMENT_SUFFIX "%c",
             &segment_id,
             &trailing) != 1) {
    return 0;
  }
  *out_segment_id = (uint64_t)segment_id;
  return 1;
}

char* observability_logger_spool_segment_path(const char* directory, uint64_t segment_id) {
  char file_name[OBS_LOGGER_SEGMENT_NAME_CAPACITY];
  if (segment_file_name(segment_id, file_name, sizeof(file_name)) <= 0) {
    return NULL;
  }
  return path_join(directory, file_name);
}

char* observability_logger_spool_cursor_directory_path(const char* directory) {
  return path_join(directory, OBS_LOGGER_CURSOR_DIRECTORY_NAME);
}

int observability_logger_spool_provider_id_is_valid(const char* provider_id) {
  uint32_t index = 0u;
  if (provider_id == NULL || provider_id[0] == '\0') {
    return 0;
  }
  while (provider_id[index] != '\0') {
    const char c = provider_id[index];
    const int is_alpha = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
    const int is_digit = c >= '0' && c <= '9';
    const int is_symbol = c == '_' || c == '-' || c == '.';
    if (index >= OBS_LOGGER_PROVIDER_ID_CAPACITY - 1u ||
        (is_alpha == 0 && is_digit == 0 && is_symbol == 0)) {
      return 0;
    }
    index += 1u;
  }
  return 1;
}

char* observability_logger_spool_provider_cursor_path(
    const char* cursor_directory,
    const char* provider_id) {
  char file_name[OBS_LOGGER_PROVIDER_ID_CAPACITY + 8u] = {};
  if (cursor_directory == NULL ||
      !observability_logger_spool_provider_id_is_valid(provider_id)) {
    return NULL;
  }
  snprintf(file_name, sizeof(file_name), "%s.cursor", provider_id);
  return path_join(cursor_directory, file_name);
}

static int encode_cursor_token(uint64_t segment_id,
                               uint64_t offset,
                               char* buffer,
                               size_t buffer_capacity) {
  const int required = snprintf(buffer,
                                buffer_capacity,
                                "%020llu:%020llu",
                                (unsigned long long)segment_id,
                                (unsigned long long)offset);
  return required > 0 && (size_t)required < buffer_capacity ? 1 : 0;
}

int observability_logger_spool_parse_cursor_token(
    const char* token,
    uint64_t* out_segment_id,
    uint64_t* out_offset) {
  unsigned long long segment_id = 0u;
  unsigned long long offset = 0u;
  int consumed = 0;
  const char* cursor = NULL;
  if (token == NULL || out_segment_id == NULL || out_offset == NULL) {
    return 0;
  }
  if (sscanf(token, "%llu:%llu%n", &segment_id, &offset, &consumed) != 2) {
    return 0;
  }
  cursor = token + consumed;
  while (*cursor == ' ' || *cursor == '\n' || *cursor == '\r' || *cursor == '\t') {
    cursor += 1;
  }
  if (*cursor != '\0') {
    return 0;
  }
  *out_segment_id = (uint64_t)segment_id;
  *out_offset = (uint64_t)offset;
  return 1;
}

static int persist_cursor_locked(const observability_logger_provider_cursor_t* cursor) {
  char token[OBS_LOGGER_CURSOR_TOKEN_CAPACITY] = {};
  char* temporary_path = NULL;
  int required = 0;
  int fd = -1;
  int ok = 0;

  /* Cursor persistence uses write-temp/fsync/rename so a crash leaves either
   * the previous ack point or the new one, never a half-written token. */
  if (cursor == NULL || cursor->cursor_file_path == NULL ||
      !encode_cursor_token(cursor->ack_segment_id,
                           cursor->ack_offset,
                           token,
                           sizeof(token))) {
    return 0;
  }

  required = snprintf(NULL, 0, "%s.tmp", cursor->cursor_file_path);
  if (required <= 0) {
    return 0;
  }
  temporary_path = (char*)calloc(1u, (size_t)required + 1u);
  if (temporary_path == NULL) {
    return 0;
  }
  snprintf(temporary_path, (size_t)required + 1u, "%s.tmp", cursor->cursor_file_path);

  fd = open(temporary_path, O_CREAT | O_WRONLY | O_TRUNC | O_CLOEXEC, 0600);
  if (fd >= 0 &&
      observability_logger_write_all_bytes(fd, token, strlen(token)) &&
      observability_logger_write_all_bytes(fd, "\n", 1u) &&
      fsync(fd) == 0 &&
      close(fd) == 0 &&
      rename(temporary_path, cursor->cursor_file_path) == 0) {
    ok = 1;
  } else if (fd >= 0) {
    close(fd);
  }
  if (ok == 0) {
    unlink(temporary_path);
  }
  free(temporary_path);
  return ok;
}

void observability_logger_spool_load_cursor_locked(
    observability_logger_t* logger,
    observability_logger_provider_cursor_t* cursor,
    uint64_t min_segment_id,
    uint64_t segment_count) {
  char buffer[OBS_LOGGER_CURSOR_TOKEN_CAPACITY] = {};
  int fd = -1;
  ssize_t bytes_read = 0;

  if (logger == NULL || cursor == NULL) {
    return;
  }

  cursor->ack_segment_id = segment_count == 0u ? logger->active_segment_id : min_segment_id;
  cursor->ack_offset = 0u;

  if (cursor->cursor_file_path == NULL) {
    return;
  }

  fd = open(cursor->cursor_file_path, O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    return;
  }
  bytes_read = read(fd, buffer, sizeof(buffer) - 1u);
  close(fd);
  if (bytes_read <= 0) {
    return;
  }
  buffer[bytes_read] = '\0';
  /* Clamp stale or corrupt cursors back into the currently retained segment
   * range. Retention can delete old unacked files by design, so the cursor must
   * never point before the oldest segment still on disk. */
  if (!observability_logger_spool_parse_cursor_token(buffer,
                                                     &cursor->ack_segment_id,
                                                     &cursor->ack_offset)) {
    cursor->ack_segment_id = segment_count == 0u ? logger->active_segment_id : min_segment_id;
    cursor->ack_offset = 0u;
  }
  if (segment_count != 0u && cursor->ack_segment_id < min_segment_id) {
    cursor->ack_segment_id = min_segment_id;
    cursor->ack_offset = 0u;
  }
  if (cursor->ack_segment_id > logger->active_segment_id) {
    cursor->ack_segment_id = logger->active_segment_id;
    cursor->ack_offset = 0u;
  }
}

observability_logger_provider_cursor_t* observability_logger_spool_get_provider_cursor_locked(
    observability_logger_t* logger,
    const char* provider_id,
    int create_if_missing) {
  uint32_t index = 0u;
  uint64_t min_segment_id = 0u;
  uint64_t max_segment_id = 0u;
  uint32_t segment_count = 0u;
  observability_logger_provider_cursor_t* cursor = NULL;

  if (logger == NULL ||
      logger->cursor_directory_path == NULL ||
      !observability_logger_spool_provider_id_is_valid(provider_id)) {
    return NULL;
  }
  for (index = 0u; index < logger->provider_cursor_count; ++index) {
    if (logger->provider_cursors[index].active != 0u &&
        strcmp(logger->provider_cursors[index].provider_id, provider_id) == 0) {
      return &logger->provider_cursors[index];
    }
  }
  if (create_if_missing == 0 ||
      logger->provider_cursor_count >= OBS_LOGGER_MAX_PROVIDER_CURSORS) {
    return NULL;
  }

  /* Provider ids are durable cursor identities, not routing rules. Every
   * provider reads the same file-first spool and owns its own ack offset:
   * one consumer can ACK while another cursor stays behind after a delivery
   * failure. Kotlin owns payload formatting and transport policy. */
  cursor = &logger->provider_cursors[logger->provider_cursor_count];
  memset(cursor, 0, sizeof(*cursor));
  cursor->active = 1u;
  snprintf(cursor->provider_id, sizeof(cursor->provider_id), "%s", provider_id);
  cursor->cursor_file_path =
      observability_logger_spool_provider_cursor_path(logger->cursor_directory_path, provider_id);
  if (cursor->cursor_file_path == NULL ||
      !observability_logger_spool_scan_segment_bounds(logger->spool_directory_path,
                                                      &min_segment_id,
                                                      &max_segment_id,
                                                      &segment_count)) {
    free(cursor->cursor_file_path);
    memset(cursor, 0, sizeof(*cursor));
    return NULL;
  }
  observability_logger_spool_load_cursor_locked(logger, cursor, min_segment_id, segment_count);
  logger->provider_cursor_count += 1u;
  return cursor;
}

int observability_logger_spool_open_segment_append_fd(const char* file_path) {
  if (file_path == NULL || file_path[0] == '\0') {
    return -1;
  }
  return open(file_path, O_CREAT | O_WRONLY | O_APPEND | O_CLOEXEC, 0600);
}

int observability_logger_spool_scan_segment_bounds(
    const char* directory,
    uint64_t* out_min_segment_id,
    uint64_t* out_max_segment_id,
    uint32_t* out_segment_count) {
  DIR* dir = NULL;
  struct dirent* entry = NULL;
  uint64_t segment_id = 0u;
  uint64_t min_segment_id = 0u;
  uint64_t max_segment_id = 0u;
  uint32_t segment_count = 0u;

  if (directory == NULL || out_min_segment_id == NULL || out_max_segment_id == NULL ||
      out_segment_count == NULL) {
    return 0;
  }

  dir = opendir(directory);
  if (dir == NULL) {
    return 0;
  }

  while ((entry = readdir(dir)) != NULL) {
    if (!parse_segment_id(entry->d_name, &segment_id)) {
      continue;
    }
    if (segment_count == 0u || segment_id < min_segment_id) {
      min_segment_id = segment_id;
    }
    if (segment_id > max_segment_id) {
      max_segment_id = segment_id;
    }
    segment_count += 1u;
  }
  closedir(dir);

  *out_min_segment_id = min_segment_id;
  *out_max_segment_id = max_segment_id;
  *out_segment_count = segment_count;
  return 1;
}

int observability_logger_spool_cleanup_old_segments_locked(
    observability_logger_t* logger) {
  const uint32_t max_segment_count = logger->max_archived_segments + 1u;
  uint64_t min_segment_id = 0u;
  uint64_t max_segment_id = 0u;
  uint32_t segment_count = 0u;
  uint32_t pruned_ack_segment = 0u;

  /* Retention is intentionally bounded and best-effort. If Kotlin has not acked
   * old data fast enough, pruning may advance the ack cursor to the oldest
   * remaining segment. That trades perfect delivery for a hard disk budget. */
  while (observability_logger_spool_scan_segment_bounds(logger->spool_directory_path,
                                                        &min_segment_id,
                                                        &max_segment_id,
                                                        &segment_count) &&
         segment_count > max_segment_count &&
         min_segment_id < logger->active_segment_id) {
    char* oldest_path =
        observability_logger_spool_segment_path(logger->spool_directory_path, min_segment_id);
    if (oldest_path == NULL) {
      return 0;
    }
    if (unlink(oldest_path) != 0 && errno != ENOENT) {
      free(oldest_path);
      return 0;
    }
    for (uint32_t index = 0u; index < logger->provider_cursor_count; ++index) {
      observability_logger_provider_cursor_t* cursor = &logger->provider_cursors[index];
      if (cursor->active != 0u && cursor->ack_segment_id <= min_segment_id) {
        pruned_ack_segment = 1u;
      }
    }
    free(oldest_path);
  }
  if (pruned_ack_segment != 0u &&
      observability_logger_spool_scan_segment_bounds(logger->spool_directory_path,
                                                     &min_segment_id,
                                                     &max_segment_id,
                                                     &segment_count) &&
      segment_count > 0u) {
    for (uint32_t index = 0u; index < logger->provider_cursor_count; ++index) {
      observability_logger_provider_cursor_t* cursor = &logger->provider_cursors[index];
      if (cursor->active == 0u || cursor->ack_segment_id >= min_segment_id) {
        continue;
      }
      cursor->ack_segment_id = min_segment_id;
      cursor->ack_offset = 0u;
      if (!persist_cursor_locked(cursor)) {
        return 0;
      }
    }
  }
  return 1;
}

int observability_logger_spool_has_unacked_records_locked(
    const observability_logger_t* logger) {
  uint32_t provider_index = 0u;

  if (logger == NULL ||
      atomic_load_explicit(&logger->started, memory_order_acquire) == 0u ||
      logger->spool_directory_path == NULL ||
      logger->active_segment_id == 0u) {
    return 0;
  }

  for (provider_index = 0u; provider_index < logger->provider_cursor_count; ++provider_index) {
    const observability_logger_provider_cursor_t* cursor = &logger->provider_cursors[provider_index];
    uint64_t segment_id = 0u;
    if (cursor->active == 0u || cursor->ack_segment_id == 0u) {
      continue;
    }
    for (segment_id = cursor->ack_segment_id;
         segment_id <= logger->active_segment_id;
         ++segment_id) {
      char* path = observability_logger_spool_segment_path(logger->spool_directory_path, segment_id);
      uint64_t size = 0u;
      if (path == NULL) {
        return 0;
      }
      size = observability_logger_spool_file_size_bytes(path);
      free(path);
      /* The native spool does not filter records by provider. A provider cursor
       * is considered pending when any retained byte exists after its ack
       * offset. If a Kotlin runner wants to skip a record, it should read the
       * batch and ACK according to its own policy. */
      if (segment_id == cursor->ack_segment_id) {
        if (size > cursor->ack_offset) {
          return 1;
        }
      } else if (size > 0u) {
        return 1;
      }
    }
  }
  return 0;
}

int observability_logger_spool_rotate_active_segment_locked(
    observability_logger_t* logger) {
  char* next_path = NULL;
  int next_fd = -1;
  const uint64_t next_segment_id = logger->active_segment_id + 1u;
  next_path =
      observability_logger_spool_segment_path(logger->spool_directory_path, next_segment_id);
  if (next_path == NULL) {
    return 0;
  }
  next_fd = observability_logger_spool_open_segment_append_fd(next_path);
  if (next_fd < 0) {
    free(next_path);
    return 0;
  }
  if (logger->active_fd >= 0) {
    close(logger->active_fd);
  }
  free(logger->active_file_path);
  logger->active_segment_id = next_segment_id;
  logger->active_file_path = next_path;
  logger->active_fd = next_fd;
  logger->current_file_size_bytes = 0u;
  logger->roll_count += 1u;
  return observability_logger_spool_cleanup_old_segments_locked(logger);
}

observability_logger_status_t observability_logger_read_next_batch(
    const char* provider_id,
    uint32_t max_records,
    uint32_t max_bytes,
    observability_logger_record_batch_t* out_batch) {
  uint64_t segment_id = 0u;
  uint64_t offset = 0u;
  uint64_t active_segment_id = 0u;
  uint32_t record_count = 0u;
  uint32_t total_bytes = 0u;
  char** records = NULL;
  char token[OBS_LOGGER_CURSOR_TOKEN_CAPACITY] = {};
  observability_logger_provider_cursor_t* cursor = NULL;

  /* readNextBatch is a peek, not a commit. It starts from the persisted ack
   * cursor and returns an ack token for the byte after the last returned line.
   * Only `ack(token)` advances durable delivery state. */
  if (out_batch == NULL || provider_id == NULL || max_records == 0u || max_bytes == 0u) {
    return OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT;
  }
  memset(out_batch, 0, sizeof(*out_batch));

  if ((size_t)max_records > SIZE_MAX / sizeof(*records)) {
    return OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT;
  }
  records = (char**)calloc(max_records, sizeof(*records));
  if (records == NULL) {
    return OBSERVABILITY_LOGGER_STATUS_INTERNAL_ERROR;
  }

  pthread_mutex_lock(&obs_logger_state.mutex);
  if (atomic_load_explicit(&obs_logger_state.started, memory_order_acquire) == 0u ||
      obs_logger_state.spool_directory_path == NULL) {
    pthread_mutex_unlock(&obs_logger_state.mutex);
    free(records);
    return OBSERVABILITY_LOGGER_STATUS_BAD_STATE;
  }
  obs_logger_state.data_available_doorbell_pending = 0u;
  /* Reading a batch consumes the coalesced DATA_AVAILABLE doorbell, but it does
   * not commit delivery progress. A failed transporter can simply avoid ACK and
   * receive the same lines again on the next drain. */
  cursor = observability_logger_spool_get_provider_cursor_locked(&obs_logger_state,
                                                                 provider_id,
                                                                 1);
  if (cursor == NULL) {
    pthread_mutex_unlock(&obs_logger_state.mutex);
    free(records);
    return OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT;
  }

  segment_id = cursor->ack_segment_id;
  offset = cursor->ack_offset;
  active_segment_id = obs_logger_state.active_segment_id;

  while (record_count < max_records && total_bytes < max_bytes && segment_id <= active_segment_id) {
    char* path = observability_logger_spool_segment_path(obs_logger_state.spool_directory_path, segment_id);
    FILE* file = NULL;
    char* line = NULL;
    size_t line_capacity = 0u;
    ssize_t line_length = 0;

    if (path == NULL) {
      pthread_mutex_unlock(&obs_logger_state.mutex);
      free(records);
      return OBSERVABILITY_LOGGER_STATUS_INTERNAL_ERROR;
    }

    file = fopen(path, "r");
    free(path);
    if (file == NULL) {
      if (segment_id < active_segment_id && errno == ENOENT) {
        segment_id += 1u;
        offset = 0u;
        continue;
      }
      break;
    }
    if (fseeko(file, (off_t)offset, SEEK_SET) != 0) {
      fclose(file);
      break;
    }

    while (record_count < max_records && total_bytes < max_bytes &&
           (line_length = getline(&line, &line_capacity, file)) > 0) {
      char* record = NULL;
      if (total_bytes != 0u && total_bytes + (uint32_t)line_length > max_bytes) {
        break;
      }
      record = (char*)calloc(1u, (size_t)line_length + 1u);
      if (record == NULL) {
        free(line);
        fclose(file);
        pthread_mutex_unlock(&obs_logger_state.mutex);
        observability_logger_record_batch_t partial = {
            .records = records,
            .record_count = record_count,
        };
        observability_logger_free_record_batch(&partial);
        return OBSERVABILITY_LOGGER_STATUS_INTERNAL_ERROR;
      }
      memcpy(record, line, (size_t)line_length);
      records[record_count] = record;
      record_count += 1u;
      total_bytes += (uint32_t)line_length;
      offset += (uint64_t)line_length;
    }
    free(line);
    fclose(file);

    if (record_count >= max_records || total_bytes >= max_bytes) {
      break;
    }
    if (segment_id >= active_segment_id) {
      break;
    }
    segment_id += 1u;
    offset = 0u;
  }

  if (record_count == 0u) {
    pthread_mutex_unlock(&obs_logger_state.mutex);
    free(records);
    return OBSERVABILITY_LOGGER_STATUS_OK;
  }
  if (!encode_cursor_token(segment_id, offset, token, sizeof(token))) {
    pthread_mutex_unlock(&obs_logger_state.mutex);
    observability_logger_record_batch_t partial = {
        .records = records,
        .record_count = record_count,
    };
    observability_logger_free_record_batch(&partial);
    return OBSERVABILITY_LOGGER_STATUS_INTERNAL_ERROR;
  }
  out_batch->ack_token = strdup(token);
  out_batch->records = records;
  out_batch->record_count = record_count;
  if (out_batch->ack_token == NULL) {
    pthread_mutex_unlock(&obs_logger_state.mutex);
    observability_logger_free_record_batch(out_batch);
    return OBSERVABILITY_LOGGER_STATUS_INTERNAL_ERROR;
  }
  pthread_mutex_unlock(&obs_logger_state.mutex);
  return OBSERVABILITY_LOGGER_STATUS_OK;
}

observability_logger_status_t observability_logger_ack(
    const char* provider_id,
    const char* ack_token) {
  uint64_t segment_id = 0u;
  uint64_t offset = 0u;
  char* path = NULL;
  uint64_t size = 0u;
  int has_more_records = 0;
  observability_logger_provider_cursor_t* cursor = NULL;

  /* Ack tokens are monotonically increasing segment:offset cursors generated by
   * readNextBatch. Kotlin treats them as opaque bytes; native validates them
   * against the current retained files before persisting the cursor. */
  if (provider_id == NULL ||
      !observability_logger_spool_parse_cursor_token(ack_token, &segment_id, &offset)) {
    return OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT;
  }

  pthread_mutex_lock(&obs_logger_state.mutex);
  if (atomic_load_explicit(&obs_logger_state.started, memory_order_acquire) == 0u ||
      obs_logger_state.spool_directory_path == NULL) {
    pthread_mutex_unlock(&obs_logger_state.mutex);
    return OBSERVABILITY_LOGGER_STATUS_BAD_STATE;
  }
  cursor = observability_logger_spool_get_provider_cursor_locked(&obs_logger_state,
                                                                 provider_id,
                                                                 0);
  if (cursor == NULL) {
    pthread_mutex_unlock(&obs_logger_state.mutex);
    return OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT;
  }
  if (segment_id < cursor->ack_segment_id ||
      (segment_id == cursor->ack_segment_id && offset <= cursor->ack_offset)) {
    pthread_mutex_unlock(&obs_logger_state.mutex);
    return OBSERVABILITY_LOGGER_STATUS_OK;
  }
  if (segment_id > obs_logger_state.active_segment_id) {
    pthread_mutex_unlock(&obs_logger_state.mutex);
    return OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT;
  }

  path = observability_logger_spool_segment_path(obs_logger_state.spool_directory_path, segment_id);
  if (path == NULL) {
    pthread_mutex_unlock(&obs_logger_state.mutex);
    return OBSERVABILITY_LOGGER_STATUS_INTERNAL_ERROR;
  }
  size = observability_logger_spool_file_size_bytes(path);
  free(path);
  if (offset > size) {
    pthread_mutex_unlock(&obs_logger_state.mutex);
    return OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT;
  }

  cursor->ack_segment_id = segment_id;
  cursor->ack_offset = offset;
  if (!persist_cursor_locked(cursor) ||
      !observability_logger_spool_cleanup_old_segments_locked(&obs_logger_state)) {
    pthread_mutex_unlock(&obs_logger_state.mutex);
    return OBSERVABILITY_LOGGER_STATUS_INTERNAL_ERROR;
  }
  has_more_records = observability_logger_spool_has_unacked_records_locked(&obs_logger_state);
  pthread_mutex_unlock(&obs_logger_state.mutex);
  if (has_more_records != 0) {
    observability_logger_core_notify_data_available_once();
  }
  return OBSERVABILITY_LOGGER_STATUS_OK;
}

void observability_logger_free_record_batch(
    observability_logger_record_batch_t* batch) {
  uint32_t index = 0u;
  if (batch == NULL) {
    return;
  }
  free(batch->ack_token);
  if (batch->records != NULL) {
    for (index = 0u; index < batch->record_count; ++index) {
      free(batch->records[index]);
    }
  }
  free(batch->records);
  memset(batch, 0, sizeof(*batch));
}
