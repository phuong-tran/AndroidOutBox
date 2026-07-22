#include "observability_logger_control.h"

#include "observability_logger_core.h"
#include "observability_logger_frame.h"
#include "observability_logger_internal.h"
#include "observability_logger_spool.h"

#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define OBS_LOGGER_CONTROL_HEADER_BYTES 24u
#define OBS_LOGGER_CONTROL_MAGIC 0x5A4F4253u
#define OBS_LOGGER_CONTROL_FRAME_MAX_BYTES \
  OBS_LOGGER_MAX_PIPE_FRAME_BYTES
#define OBS_LOGGER_CONTROL_POLL_TIMEOUT_MS 100
#define OBS_LOGGER_CONTROL_VERSION 1u
#define OBS_LOGGER_CONTROL_COMMAND_CONFIGURE 1u
#define OBS_LOGGER_CONTROL_COMMAND_FLUSH 2u
#define OBS_LOGGER_CONTROL_COMMAND_STOP 3u
#define OBS_LOGGER_CONTROL_COMMAND_READ_BATCH 4u
#define OBS_LOGGER_CONTROL_COMMAND_ACK 5u
#define OBS_LOGGER_CONTROL_COMMAND_WRITE 6u
#define OBS_LOGGER_CONTROL_COMMAND_GET_STATS 7u
#define OBS_LOGGER_CONTROL_COMMAND_CLOSE_PIPES 8u
#define OBS_LOGGER_CONTROL_RESPONSE_HEADER_BYTES 16u
#define OBS_LOGGER_CONTROL_FIELD_SPOOL_DIRECTORY_PATH 1u
#define OBS_LOGGER_CONTROL_FIELD_QUEUE_CAPACITY 2u
#define OBS_LOGGER_CONTROL_FIELD_MAX_RECORD_BYTES 3u
#define OBS_LOGGER_CONTROL_FIELD_MAX_SEGMENT_SIZE_BYTES 4u
#define OBS_LOGGER_CONTROL_FIELD_MAX_ARCHIVED_SEGMENTS 5u
#define OBS_LOGGER_CONTROL_FIELD_MAX_BATCH_RECORDS 6u
#define OBS_LOGGER_CONTROL_FIELD_MAX_BATCH_BYTES 7u
#define OBS_LOGGER_CONTROL_FIELD_ACK_TOKEN 8u
#define OBS_LOGGER_CONTROL_FIELD_LOG_LEVEL 9u
#define OBS_LOGGER_CONTROL_FIELD_LOG_CATEGORY 10u
#define OBS_LOGGER_CONTROL_FIELD_LOG_PAYLOAD 11u
#define OBS_LOGGER_CONTROL_FIELD_PROVIDER_ID 12u
#define OBS_LOGGER_CONTROL_VALUE_STRING 1u
#define OBS_LOGGER_CONTROL_VALUE_UINT32 2u
#define OBS_LOGGER_CONTROL_VALUE_UINT64 3u
#define OBS_LOGGER_CONTROL_VALUE_BYTES 4u

typedef struct observability_logger_control_command_frame_t {
  uint64_t sequence;
  uint32_t command;
  const uint8_t* payload;
  uint32_t payload_length;
} observability_logger_control_command_frame_t;

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

void observability_logger_control_notify_if_unacked_records_available(void) {
  int has_records = 0;
  pthread_mutex_lock(&obs_logger_state.mutex);
  has_records = observability_logger_spool_has_unacked_records_locked(&obs_logger_state);
  pthread_mutex_unlock(&obs_logger_state.mutex);
  if (has_records != 0) {
    observability_logger_core_notify_data_available_once();
  }
}

static int write_control_response(uint64_t sequence,
                                  uint32_t command,
                                  observability_logger_status_t status,
                                  const uint8_t* body,
                                  uint32_t body_length) {
  uint32_t payload_length = 0u;
  uint8_t* frame = NULL;
  uint32_t offset = 0u;
  int record_write_fd = -1;
  int ok = 0;

  if (body_length > OBS_LOGGER_MAX_PIPE_FRAME_BYTES - OBS_LOGGER_CONTROL_RESPONSE_HEADER_BYTES) {
    return 0;
  }
  payload_length = OBS_LOGGER_CONTROL_RESPONSE_HEADER_BYTES + body_length;
  frame = (uint8_t*)calloc(1u, (size_t)OBS_LOGGER_FRAME_HEADER_BYTES + payload_length);
  if (frame == NULL) {
    return 0;
  }
  observability_logger_write_u32_be(payload_length, frame);
  offset = OBS_LOGGER_FRAME_HEADER_BYTES;
  observability_logger_write_u64_le(sequence, frame + offset);
  offset += 8u;
  observability_logger_write_u32_le(command, frame + offset);
  offset += 4u;
  observability_logger_write_u32_le((uint32_t)status, frame + offset);
  offset += 4u;
  if (body_length > 0u && body != NULL) {
    memcpy(frame + offset, body, body_length);
  }

  /* The control thread is the only response writer. Keep the potentially
   * blocking pipe write outside the logger mutex so close/stop and the file
   * writer are not held hostage by a slow Kotlin reader. If this ever gains a
   * second native response writer, add a dedicated response-write mutex rather
   * than reusing the logger state mutex. */
  pthread_mutex_lock(&obs_logger_state.mutex);
  record_write_fd = obs_logger_state.record_write_fd;
  pthread_mutex_unlock(&obs_logger_state.mutex);
  if (record_write_fd >= 0) {
    ok = observability_logger_write_pipe_all_bytes(record_write_fd,
                                                   frame,
                                                   (size_t)OBS_LOGGER_FRAME_HEADER_BYTES +
                                                       payload_length);
  }
  free(frame);
  return ok;
}

static int write_empty_control_response(uint64_t sequence,
                                        uint32_t command,
                                        observability_logger_status_t status) {
  return write_control_response(sequence, command, status, NULL, 0u);
}

static int write_record_batch_response(
    uint64_t sequence,
    uint32_t command,
    observability_logger_status_t status,
    const observability_logger_record_batch_t* batch) {
  uint32_t ack_token_length = 0u;
  uint32_t record_count = 0u;
  const size_t max_body_length =
      OBS_LOGGER_MAX_PIPE_FRAME_BYTES - OBS_LOGGER_CONTROL_RESPONSE_HEADER_BYTES;
  size_t body_length = 8u;
  uint32_t index = 0u;
  size_t offset = 0u;
  uint8_t* body = NULL;
  int ok = 0;

  if (status == OBSERVABILITY_LOGGER_STATUS_OK && batch != NULL &&
      batch->ack_token != NULL && batch->record_count > 0u) {
    ack_token_length = bounded_strlen(batch->ack_token, OBS_LOGGER_CURSOR_TOKEN_CAPACITY);
    record_count = batch->record_count;
    if (body_length > max_body_length ||
        (size_t)ack_token_length > max_body_length - body_length) {
      return 0;
    }
    body_length += ack_token_length;
    for (index = 0u; index < record_count; ++index) {
      uint32_t record_length = 0u;
      if (batch->records[index] == NULL) {
        return 0;
      }
      record_length = bounded_strlen(batch->records[index], UINT32_MAX);
      if (body_length > max_body_length ||
          max_body_length - body_length < 4u ||
          (size_t)record_length > max_body_length - body_length - 4u) {
        return 0;
      }
      body_length += 4u + (size_t)record_length;
    }
  }

  body = (uint8_t*)calloc(1u, body_length);
  if (body == NULL) {
    return 0;
  }
  observability_logger_write_u32_le(ack_token_length, body + offset);
  offset += 4u;
  observability_logger_write_u32_le(record_count, body + offset);
  offset += 4u;
  if (ack_token_length > 0u) {
    memcpy(body + offset, batch->ack_token, ack_token_length);
    offset += ack_token_length;
  }
  for (index = 0u; index < record_count; ++index) {
    const uint32_t record_length = bounded_strlen(batch->records[index], UINT32_MAX);
    observability_logger_write_u32_le(record_length, body + offset);
    offset += 4u;
    memcpy(body + offset, batch->records[index], record_length);
    offset += record_length;
  }

  ok = write_control_response(sequence, command, status, body, (uint32_t)body_length);
  free(body);
  return ok;
}

static observability_logger_status_t write_read_batch_error_response(
    uint64_t sequence,
    uint32_t command,
    observability_logger_status_t status) {
  return write_record_batch_response(sequence, command, status, NULL) != 0
             ? status
             : OBSERVABILITY_LOGGER_STATUS_INTERNAL_ERROR;
}

/* The pipe transports opaque length-prefixed frames. Command shape is decoded
 * only after a full frame has been received, which keeps stream IO independent
 * from control-plane schema evolution. */
static observability_logger_status_t decode_control_command_frame(
    const uint8_t* frame,
    uint32_t frame_length,
    observability_logger_control_command_frame_t* out_command) {
  uint32_t magic = 0u;
  uint32_t version = 0u;
  uint32_t payload_length = 0u;

  if (frame == NULL || out_command == NULL || frame_length < OBS_LOGGER_CONTROL_HEADER_BYTES) {
    return OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT;
  }
  memset(out_command, 0, sizeof(*out_command));

  magic = observability_logger_read_u32_le(frame);
  version = observability_logger_read_u32_le(frame + 4u);
  out_command->command = observability_logger_read_u32_le(frame + 8u);
  payload_length = observability_logger_read_u32_le(frame + 12u);
  out_command->sequence = observability_logger_read_u64_le(frame + 16u);
  if (magic != OBS_LOGGER_CONTROL_MAGIC ||
      version != OBS_LOGGER_CONTROL_VERSION ||
      payload_length > OBS_LOGGER_CONTROL_FRAME_MAX_BYTES - OBS_LOGGER_CONTROL_HEADER_BYTES ||
      frame_length - OBS_LOGGER_CONTROL_HEADER_BYTES != payload_length) {
    return OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT;
  }

  out_command->payload_length = payload_length;
  out_command->payload = payload_length > 0u ? frame + OBS_LOGGER_CONTROL_HEADER_BYTES : NULL;
  return OBSERVABILITY_LOGGER_STATUS_OK;
}

static observability_logger_status_t apply_configure_payload(
    const uint8_t* payload,
    uint32_t payload_length) {
  uint32_t field_count = 0u;
  uint32_t field_index = 0u;
  uint32_t offset = 0u;
  char* spool_directory_path = NULL;
  char* default_provider_id = NULL;
  observability_logger_config_t config = {};
  observability_logger_status_t status = OBSERVABILITY_LOGGER_STATUS_OK;

  if (payload == NULL || payload_length < 4u) {
    return OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT;
  }

  field_count = observability_logger_read_u32_le(payload);
  offset = 4u;
  for (field_index = 0u; field_index < field_count; ++field_index) {
    uint32_t field_id = 0u;
    uint32_t value_type = 0u;
    uint32_t value_length = 0u;
    const uint8_t* value = NULL;

    if (offset > payload_length || payload_length - offset < 12u) {
      free(spool_directory_path);
      free(default_provider_id);
      return OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT;
    }

    field_id = observability_logger_read_u32_le(payload + offset);
    value_type = observability_logger_read_u32_le(payload + offset + 4u);
    value_length = observability_logger_read_u32_le(payload + offset + 8u);
    offset += 12u;
    if (offset > payload_length || value_length > payload_length - offset) {
      free(spool_directory_path);
      free(default_provider_id);
      return OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT;
    }

    value = payload + offset;
    switch (field_id) {
      case OBS_LOGGER_CONTROL_FIELD_SPOOL_DIRECTORY_PATH:
        if (value_type != OBS_LOGGER_CONTROL_VALUE_STRING || value_length == 0u) {
          free(spool_directory_path);
          free(default_provider_id);
          return OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT;
        }
        free(spool_directory_path);
        spool_directory_path = (char*)calloc(1u, (size_t)value_length + 1u);
        if (spool_directory_path == NULL) {
          free(default_provider_id);
          return OBSERVABILITY_LOGGER_STATUS_INTERNAL_ERROR;
        }
        memcpy(spool_directory_path, value, value_length);
        break;
      case OBS_LOGGER_CONTROL_FIELD_PROVIDER_ID:
        if (value_type != OBS_LOGGER_CONTROL_VALUE_STRING || value_length == 0u) {
          free(spool_directory_path);
          free(default_provider_id);
          return OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT;
        }
        free(default_provider_id);
        default_provider_id = (char*)calloc(1u, (size_t)value_length + 1u);
        if (default_provider_id == NULL) {
          free(spool_directory_path);
          return OBSERVABILITY_LOGGER_STATUS_INTERNAL_ERROR;
        }
        memcpy(default_provider_id, value, value_length);
        break;
      case OBS_LOGGER_CONTROL_FIELD_QUEUE_CAPACITY:
        if (value_type == OBS_LOGGER_CONTROL_VALUE_UINT32 && value_length == 4u) {
          config.queue_capacity = observability_logger_read_u32_le(value);
        }
        break;
      case OBS_LOGGER_CONTROL_FIELD_MAX_RECORD_BYTES:
        if (value_type == OBS_LOGGER_CONTROL_VALUE_UINT32 && value_length == 4u) {
          config.max_record_bytes = observability_logger_read_u32_le(value);
        }
        break;
      case OBS_LOGGER_CONTROL_FIELD_MAX_SEGMENT_SIZE_BYTES:
        if (value_type == OBS_LOGGER_CONTROL_VALUE_UINT64 && value_length == 8u) {
          config.max_segment_size_bytes = observability_logger_read_u64_le(value);
        }
        break;
      case OBS_LOGGER_CONTROL_FIELD_MAX_ARCHIVED_SEGMENTS:
        if (value_type == OBS_LOGGER_CONTROL_VALUE_UINT32 && value_length == 4u) {
          config.max_archived_segments = observability_logger_read_u32_le(value);
        }
        break;
      default:
        break;
    }
    offset += value_length;
  }

  config.spool_directory_path = spool_directory_path;
  config.default_provider_id = default_provider_id;
  status = observability_logger_start(&config);
  free(spool_directory_path);
  free(default_provider_id);
  if (status == OBSERVABILITY_LOGGER_STATUS_OK) {
    observability_logger_control_notify_if_unacked_records_available();
  }
  return status;
}

static observability_logger_status_t apply_log_payload(const uint8_t* payload,
                                                              uint32_t payload_length) {
  uint32_t field_count = 0u;
  uint32_t field_index = 0u;
  uint32_t offset = 0u;
  int32_t level = 0;
  uint32_t has_level = 0u;
  char* category = NULL;
  char* record_payload = NULL;
  observability_logger_status_t status =
      OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT;

  if (payload == NULL || payload_length < 4u) {
    return OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT;
  }

  field_count = observability_logger_read_u32_le(payload);
  offset = 4u;
  for (field_index = 0u; field_index < field_count; ++field_index) {
    uint32_t field_id = 0u;
    uint32_t value_type = 0u;
    uint32_t value_length = 0u;
    const uint8_t* value = NULL;

    if (offset > payload_length || payload_length - offset < 12u) {
      free(category);
      free(record_payload);
      return OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT;
    }
    field_id = observability_logger_read_u32_le(payload + offset);
    value_type = observability_logger_read_u32_le(payload + offset + 4u);
    value_length = observability_logger_read_u32_le(payload + offset + 8u);
    offset += 12u;
    if (offset > payload_length || value_length > payload_length - offset) {
      free(category);
      free(record_payload);
      return OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT;
    }

    value = payload + offset;
    if (field_id == OBS_LOGGER_CONTROL_FIELD_LOG_LEVEL &&
        value_type == OBS_LOGGER_CONTROL_VALUE_UINT32 &&
        value_length == 4u) {
      level = (int32_t)observability_logger_read_u32_le(value);
      has_level = 1u;
    } else if ((field_id == OBS_LOGGER_CONTROL_FIELD_LOG_CATEGORY ||
                field_id == OBS_LOGGER_CONTROL_FIELD_LOG_PAYLOAD) &&
               value_type == OBS_LOGGER_CONTROL_VALUE_STRING &&
               value_length > 0u) {
      char* copy = (char*)calloc(1u, (size_t)value_length + 1u);
      if (copy == NULL) {
        free(category);
        free(record_payload);
        return OBSERVABILITY_LOGGER_STATUS_INTERNAL_ERROR;
      }
      memcpy(copy, value, value_length);
      if (field_id == OBS_LOGGER_CONTROL_FIELD_LOG_CATEGORY) {
        free(category);
        category = copy;
      } else {
        free(record_payload);
        record_payload = copy;
      }
    }
    offset += value_length;
  }

  if (has_level != 0u && category != NULL && record_payload != NULL) {
    status = observability_logger_log(level, category, record_payload);
  }
  free(category);
  free(record_payload);
  return status;
}

static observability_logger_status_t apply_read_batch_payload(uint64_t sequence,
                                                                     uint32_t command,
                                                                     const uint8_t* payload,
                                                                     uint32_t payload_length) {
  uint32_t field_count = 0u;
  uint32_t field_index = 0u;
  uint32_t offset = 0u;
  uint32_t max_records = 0u;
  uint32_t max_bytes = 0u;
  char* provider_id = NULL;
  observability_logger_record_batch_t batch = {};
  observability_logger_status_t status = OBSERVABILITY_LOGGER_STATUS_OK;

  if (payload == NULL || payload_length < 4u) {
    return write_read_batch_error_response(sequence,
                                           command,
                                           OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT);
  }

  field_count = observability_logger_read_u32_le(payload);
  offset = 4u;
  for (field_index = 0u; field_index < field_count; ++field_index) {
    uint32_t field_id = 0u;
    uint32_t value_type = 0u;
    uint32_t value_length = 0u;
    const uint8_t* value = NULL;

    if (offset > payload_length || payload_length - offset < 12u) {
      free(provider_id);
      return write_read_batch_error_response(sequence,
                                             command,
                                             OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT);
    }
    field_id = observability_logger_read_u32_le(payload + offset);
    value_type = observability_logger_read_u32_le(payload + offset + 4u);
    value_length = observability_logger_read_u32_le(payload + offset + 8u);
    offset += 12u;
    if (offset > payload_length || value_length > payload_length - offset) {
      free(provider_id);
      return write_read_batch_error_response(sequence,
                                             command,
                                             OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT);
    }
    value = payload + offset;
    if (value_type == OBS_LOGGER_CONTROL_VALUE_UINT32 && value_length == 4u) {
      if (field_id == OBS_LOGGER_CONTROL_FIELD_MAX_BATCH_RECORDS) {
        max_records = observability_logger_read_u32_le(value);
      } else if (field_id == OBS_LOGGER_CONTROL_FIELD_MAX_BATCH_BYTES) {
        max_bytes = observability_logger_read_u32_le(value);
      }
    } else if (field_id == OBS_LOGGER_CONTROL_FIELD_PROVIDER_ID &&
               value_type == OBS_LOGGER_CONTROL_VALUE_STRING &&
               value_length > 0u) {
      free(provider_id);
      provider_id = (char*)calloc(1u, (size_t)value_length + 1u);
      if (provider_id == NULL) {
        return write_read_batch_error_response(sequence,
                                               command,
                                               OBSERVABILITY_LOGGER_STATUS_INTERNAL_ERROR);
      }
      memcpy(provider_id, value, value_length);
    }
    offset += value_length;
  }

  if (provider_id == NULL || max_records == 0u || max_bytes == 0u) {
    free(provider_id);
    return write_read_batch_error_response(sequence,
                                           command,
                                           OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT);
  }
  status = observability_logger_read_next_batch(provider_id, max_records, max_bytes, &batch);
  if (!write_record_batch_response(sequence, command, status, &batch)) {
    status = OBSERVABILITY_LOGGER_STATUS_INTERNAL_ERROR;
  }
  free(provider_id);
  observability_logger_free_record_batch(&batch);
  return status;
}

static observability_logger_status_t apply_ack_payload(const uint8_t* payload,
                                                              uint32_t payload_length) {
  uint32_t field_count = 0u;
  uint32_t field_index = 0u;
  uint32_t offset = 0u;
  char* ack_token = NULL;
  char* provider_id = NULL;
  observability_logger_status_t status =
      OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT;

  if (payload == NULL || payload_length < 4u) {
    return OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT;
  }

  field_count = observability_logger_read_u32_le(payload);
  offset = 4u;
  for (field_index = 0u; field_index < field_count; ++field_index) {
    uint32_t field_id = 0u;
    uint32_t value_type = 0u;
    uint32_t value_length = 0u;
    const uint8_t* value = NULL;

    if (offset > payload_length || payload_length - offset < 12u) {
      free(ack_token);
      free(provider_id);
      return OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT;
    }
    field_id = observability_logger_read_u32_le(payload + offset);
    value_type = observability_logger_read_u32_le(payload + offset + 4u);
    value_length = observability_logger_read_u32_le(payload + offset + 8u);
    offset += 12u;
    if (offset > payload_length || value_length > payload_length - offset) {
      free(ack_token);
      free(provider_id);
      return OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT;
    }
    value = payload + offset;
    if (field_id == OBS_LOGGER_CONTROL_FIELD_ACK_TOKEN &&
        value_type == OBS_LOGGER_CONTROL_VALUE_BYTES &&
        value_length > 0u) {
      free(ack_token);
      ack_token = (char*)calloc(1u, (size_t)value_length + 1u);
      if (ack_token == NULL) {
        free(provider_id);
        return OBSERVABILITY_LOGGER_STATUS_INTERNAL_ERROR;
      }
      memcpy(ack_token, value, value_length);
    } else if (field_id == OBS_LOGGER_CONTROL_FIELD_PROVIDER_ID &&
               value_type == OBS_LOGGER_CONTROL_VALUE_STRING &&
               value_length > 0u) {
      free(provider_id);
      provider_id = (char*)calloc(1u, (size_t)value_length + 1u);
      if (provider_id == NULL) {
        free(ack_token);
        return OBSERVABILITY_LOGGER_STATUS_INTERNAL_ERROR;
      }
      memcpy(provider_id, value, value_length);
    }
    offset += value_length;
  }

  if (provider_id != NULL && ack_token != NULL) {
    status = observability_logger_ack(provider_id, ack_token);
  }
  free(provider_id);
  free(ack_token);
  return status;
}

static observability_logger_status_t write_stats_response(uint64_t sequence,
                                                                 uint32_t command) {
  observability_logger_stats_t stats = {};
  uint8_t body[OBS_LOGGER_STATS_FIELD_COUNT * 8u] = {};
  uint32_t offset = 0u;

  observability_logger_get_stats(&stats);
  observability_logger_write_u64_le(stats.started, body + offset);
  offset += 8u;
  observability_logger_write_u64_le(stats.queue_capacity, body + offset);
  offset += 8u;
  observability_logger_write_u64_le(stats.queue_depth, body + offset);
  offset += 8u;
  observability_logger_write_u64_le(stats.queue_high_watermark, body + offset);
  offset += 8u;
  observability_logger_write_u64_le(stats.next_sequence, body + offset);
  offset += 8u;
  observability_logger_write_u64_le(stats.accepted_count, body + offset);
  offset += 8u;
  observability_logger_write_u64_le(stats.written_count, body + offset);
  offset += 8u;
  observability_logger_write_u64_le(stats.dropped_queue_full_count, body + offset);
  offset += 8u;
  observability_logger_write_u64_le(stats.dropped_invalid_count, body + offset);
  offset += 8u;
  observability_logger_write_u64_le(stats.dropped_record_too_large_count, body + offset);
  offset += 8u;
  observability_logger_write_u64_le(stats.write_failure_count, body + offset);
  offset += 8u;
  observability_logger_write_u64_le(stats.current_file_size_bytes, body + offset);
  offset += 8u;
  observability_logger_write_u64_le(stats.roll_count, body + offset);
  return write_control_response(sequence,
                                command,
                                OBSERVABILITY_LOGGER_STATUS_OK,
                                body,
                                sizeof(body)) != 0
             ? OBSERVABILITY_LOGGER_STATUS_OK
             : OBSERVABILITY_LOGGER_STATUS_INTERNAL_ERROR;
}

static observability_logger_status_t handle_control_command(
    uint64_t sequence,
    uint32_t command,
    const uint8_t* payload,
    uint32_t payload_length,
    uint32_t* out_should_close) {
  observability_logger_status_t status = OBSERVABILITY_LOGGER_STATUS_OK;
  if (out_should_close != NULL) {
    *out_should_close = 0u;
  }
  switch (command) {
    case OBS_LOGGER_CONTROL_COMMAND_CONFIGURE:
      status = apply_configure_payload(payload, payload_length);
      break;
    case OBS_LOGGER_CONTROL_COMMAND_FLUSH:
      status = observability_logger_flush();
      break;
    case OBS_LOGGER_CONTROL_COMMAND_STOP:
      observability_logger_stop();
      status = OBSERVABILITY_LOGGER_STATUS_OK;
      break;
    case OBS_LOGGER_CONTROL_COMMAND_READ_BATCH:
      return apply_read_batch_payload(sequence, command, payload, payload_length);
    case OBS_LOGGER_CONTROL_COMMAND_ACK:
      status = apply_ack_payload(payload, payload_length);
      break;
    case OBS_LOGGER_CONTROL_COMMAND_WRITE:
      /* WRITE is intentionally one-way. Kotlin writes the command frame and returns
       * without waiting for a response, while native reports pressure through
       * counters and DROPPED_RECORD doorbells. Control commands still use request
       * response sequencing. */
      return apply_log_payload(payload, payload_length);
    case OBS_LOGGER_CONTROL_COMMAND_GET_STATS:
      return write_stats_response(sequence, command);
    case OBS_LOGGER_CONTROL_COMMAND_CLOSE_PIPES:
      if (out_should_close != NULL) {
        *out_should_close = 1u;
      }
      status = OBSERVABILITY_LOGGER_STATUS_OK;
      break;
    default:
      status = OBSERVABILITY_LOGGER_STATUS_INVALID_ARGUMENT;
      break;
  }
  if (!write_empty_control_response(sequence, command, status)) {
    return OBSERVABILITY_LOGGER_STATUS_INTERNAL_ERROR;
  }
  return status;
}

void* observability_logger_control_main(void* opaque) {
  observability_logger_t* logger = (observability_logger_t*)opaque;
  const int fd = logger->command_read_fd;
  observability_logger_frame_reader_t reader = {};
  uint32_t should_exit = 0u;

  /* One control thread serializes Kotlin commands. That keeps ordering simple:
   * each command frame produces at most one response frame with the same
   * sequence id, and commands cannot race each other inside native state. */
  observability_logger_frame_reader_init(&reader, fd, OBS_LOGGER_CONTROL_FRAME_MAX_BYTES);

  while (should_exit == 0u) {
    observability_logger_frame_read_status_t wait_status =
        OBS_LOGGER_FRAME_READ_WOULD_BLOCK;
    pthread_mutex_lock(&logger->mutex);
    if (logger->control_running == 0u) {
      pthread_mutex_unlock(&logger->mutex);
      break;
    }
    pthread_mutex_unlock(&logger->mutex);

    /* poll() keeps this single-lane control thread cheap while still letting
     * the frame reader preserve partial header/payload state across EAGAIN. */
    wait_status = observability_logger_wait_readable_fd(fd, OBS_LOGGER_CONTROL_POLL_TIMEOUT_MS);
    if (wait_status == OBS_LOGGER_FRAME_READ_WOULD_BLOCK) {
      continue;
    }
    if (wait_status != OBS_LOGGER_FRAME_READ_OK) {
      break;
    }

    for (;;) {
      uint8_t* frame = NULL;
      uint32_t frame_length = 0u;
      uint32_t should_close = 0u;
      observability_logger_control_command_frame_t command_frame = {};
      observability_logger_status_t decode_status =
          OBSERVABILITY_LOGGER_STATUS_OK;
      observability_logger_frame_read_status_t read_status =
          observability_logger_frame_reader_read_try(&reader, &frame, &frame_length);

      if (read_status == OBS_LOGGER_FRAME_READ_WOULD_BLOCK) {
        break;
      }
      if (read_status != OBS_LOGGER_FRAME_READ_OK) {
        should_exit = 1u;
        break;
      }

      decode_status = decode_control_command_frame(frame, frame_length, &command_frame);
      if (decode_status != OBSERVABILITY_LOGGER_STATUS_OK) {
        free(frame);
        should_exit = 1u;
        break;
      }

      handle_control_command(command_frame.sequence,
                             command_frame.command,
                             command_frame.payload,
                             command_frame.payload_length,
                             &should_close);
      free(frame);
      if (should_close != 0u) {
        should_exit = 1u;
        break;
      }
    }
  }

  observability_logger_frame_reader_reset(&reader);
  observability_logger_close_pipes();
  return NULL;
}
