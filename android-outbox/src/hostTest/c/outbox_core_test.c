#include "outbox_core.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define ASSERT_TRUE(condition)                                                     \
  do {                                                                            \
    if (!(condition)) {                                                           \
      fprintf(stderr, "Assertion failed at %s:%d: %s\n", __FILE__, __LINE__, #condition); \
      return 1;                                                                   \
    }                                                                             \
  } while (0)

#define ASSERT_STATUS_OK(expression) \
  ASSERT_TRUE((expression) == OUTBOX_STATUS_OK)

#define TEST_CONTROL_HEADER_BYTES 24u
#define TEST_CONTROL_MAGIC 0x5A4F4253u
#define TEST_CONTROL_VERSION 1u
#define TEST_CONTROL_COMMAND_FLUSH 2u
#define TEST_CONTROL_COMMAND_READ_BATCH 4u
#define TEST_CONTROL_COMMAND_ACK 5u
#define TEST_CONTROL_COMMAND_WRITE 6u
#define TEST_CONTROL_COMMAND_GET_STATS 7u
#define TEST_CONTROL_COMMAND_CLOSE_PIPES 8u
#define TEST_CONTROL_FIELD_MAX_BATCH_RECORDS 6u
#define TEST_CONTROL_FIELD_MAX_BATCH_BYTES 7u
#define TEST_CONTROL_FIELD_ACK_TOKEN 8u
#define TEST_CONTROL_FIELD_LOG_LEVEL 9u
#define TEST_CONTROL_FIELD_LOG_CATEGORY 10u
#define TEST_CONTROL_FIELD_LOG_PAYLOAD 11u
#define TEST_CONTROL_FIELD_PROVIDER_ID 12u
#define TEST_CONTROL_VALUE_STRING 1u
#define TEST_CONTROL_VALUE_UINT32 2u
#define TEST_CONTROL_VALUE_BYTES 4u
#define TEST_FRAME_HEADER_BYTES 4u
#define TEST_CONTROL_RESPONSE_HEADER_BYTES 16u
#define TEST_RECORD_RESPONSE_HEADER_BYTES 8u
#define TEST_STATS_BODY_BYTES (13u * 8u)
#define TEST_DOORBELL_HANDSHAKE 0u
#define TEST_DOORBELL_DATA_AVAILABLE 1u
#define TEST_PRIMARY_PROVIDER_ID "primary"
#define TEST_SECONDARY_PROVIDER_ID "secondary"

typedef struct test_context_t {
  char spool_dir[256];
} test_context_t;

typedef struct test_control_response_t {
  uint64_t sequence;
  uint32_t command;
  uint32_t status;
  unsigned char* body;
  uint32_t body_length;
} test_control_response_t;

typedef struct concurrent_log_worker_t {
  uint32_t worker_id;
  uint32_t record_count;
  uint64_t queue_full_retry_count;
  int failed;
} concurrent_log_worker_t;

typedef struct concurrent_stop_worker_t {
  int failed;
} concurrent_stop_worker_t;

typedef struct test_report_t {
  uint32_t concurrent_worker_count;
  uint32_t concurrent_records_per_worker;
  uint32_t concurrent_queue_capacity;
  uint32_t concurrent_high_watermark;
  uint32_t large_payload_bytes;
  uint32_t single_primary_records_written;
  uint32_t single_primary_delivered_records;
  uint32_t single_primary_acked_batches;
  uint32_t single_primary_retried_after_restart;
  uint32_t multi_primary_delivered_records;
  uint32_t multi_primary_acked_batches;
  uint32_t multi_secondary_delivered_records;
  uint32_t multi_secondary_acked_batches;
  uint32_t multi_cursor_independence_verified;
  uint64_t concurrent_total_records;
  uint64_t concurrent_queue_full_retries;
  uint64_t concurrent_elapsed_ns;
} test_report_t;

static test_report_t test_report = {};

static int remove_tree(const char* path) {
  DIR* dir = opendir(path);
  struct dirent* entry = NULL;
  if (dir == NULL) {
    return errno == ENOENT ? 0 : -1;
  }
  while ((entry = readdir(dir)) != NULL) {
    char child[512] = {};
    struct stat st = {};
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    snprintf(child, sizeof(child), "%s/%s", path, entry->d_name);
    if (lstat(child, &st) != 0) {
      closedir(dir);
      return -1;
    }
    if (S_ISDIR(st.st_mode)) {
      if (remove_tree(child) != 0) {
        closedir(dir);
        return -1;
      }
    } else if (unlink(child) != 0) {
      closedir(dir);
      return -1;
    }
  }
  closedir(dir);
  return rmdir(path);
}

static int setup_context(test_context_t* context) {
  snprintf(context->spool_dir, sizeof(context->spool_dir), "/tmp/outbox_test_XXXXXX");
  return mkdtemp(context->spool_dir) != NULL ? 1 : 0;
}

static void teardown_context(test_context_t* context) {
  outbox_close_pipes();
  outbox_stop();
  remove_tree(context->spool_dir);
}

static uint64_t monotonic_ns(void) {
  struct timespec ts = {};
  if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
    return 0u;
  }
  return ((uint64_t)ts.tv_sec * 1000000000ull) + (uint64_t)ts.tv_nsec;
}

static outbox_config_t config_for(const test_context_t* context) {
  outbox_config_t config = {};
  config.spool_directory_path = context->spool_dir;
  config.default_provider_id = TEST_PRIMARY_PROVIDER_ID;
  config.queue_capacity = 8u;
  config.max_record_bytes = 512u;
  config.max_segment_size_bytes = 4096u;
  config.max_archived_segments = 3u;
  return config;
}

static int count_segment_files(const char* directory, uint32_t* out_count) {
  DIR* dir = NULL;
  struct dirent* entry = NULL;
  uint32_t count = 0u;
  if (directory == NULL || out_count == NULL) {
    return 0;
  }
  dir = opendir(directory);
  if (dir == NULL) {
    return 0;
  }
  while ((entry = readdir(dir)) != NULL) {
    unsigned long long segment_id = 0u;
    char trailing = '\0';
    if (sscanf(entry->d_name, "segment-%llu.log%c", &segment_id, &trailing) == 1) {
      count += 1u;
    }
  }
  closedir(dir);
  *out_count = count;
  return 1;
}

static int start_logger(test_context_t* context) {
  outbox_config_t config = config_for(context);
  return outbox_start(&config) == OUTBOX_STATUS_OK;
}

static int log_and_flush(const char* category, const char* payload) {
  return outbox_log(4, category, payload) ==
             OUTBOX_STATUS_OK &&
         outbox_flush() == OUTBOX_STATUS_OK;
}

static void* concurrent_log_worker_main(void* opaque) {
  concurrent_log_worker_t* worker = (concurrent_log_worker_t*)opaque;
  uint32_t index = 0u;
  for (index = 0u; index < worker->record_count; ++index) {
    char payload[96] = {};
    outbox_status_t status;
    snprintf(payload,
             sizeof(payload),
             "worker=%u record=%u",
             worker->worker_id,
             index);
    do {
      status = outbox_log(4, "network.concurrent", payload);
      if (status == OUTBOX_STATUS_QUEUE_FULL) {
        worker->queue_full_retry_count += 1u;
        usleep(1000u);
      }
    } while (status == OUTBOX_STATUS_QUEUE_FULL);
    if (status != OUTBOX_STATUS_OK) {
      worker->failed = 1;
      return NULL;
    }
  }
  return NULL;
}

static void* concurrent_stop_worker_main(void* opaque) {
  concurrent_stop_worker_t* worker = (concurrent_stop_worker_t*)opaque;
  outbox_stop();
  if (worker == NULL) {
    return NULL;
  }
  worker->failed = 0;
  return NULL;
}

static uint32_t read_u32_be(const unsigned char* data) {
  return ((uint32_t)data[0] << 24u) | ((uint32_t)data[1] << 16u) |
         ((uint32_t)data[2] << 8u) | (uint32_t)data[3];
}

static uint32_t read_u32_le(const unsigned char* data) {
  return (uint32_t)data[0] | ((uint32_t)data[1] << 8u) |
         ((uint32_t)data[2] << 16u) | ((uint32_t)data[3] << 24u);
}

static uint64_t read_u64_le(const unsigned char* data) {
  return (uint64_t)read_u32_le(data) | ((uint64_t)read_u32_le(data + 4u) << 32u);
}

static void write_u32_le(unsigned char* data, uint32_t value) {
  data[0] = (unsigned char)(value & 0xffu);
  data[1] = (unsigned char)((value >> 8u) & 0xffu);
  data[2] = (unsigned char)((value >> 16u) & 0xffu);
  data[3] = (unsigned char)((value >> 24u) & 0xffu);
}

static void write_u32_be(unsigned char* data, uint32_t value) {
  data[0] = (unsigned char)((value >> 24u) & 0xffu);
  data[1] = (unsigned char)((value >> 16u) & 0xffu);
  data[2] = (unsigned char)((value >> 8u) & 0xffu);
  data[3] = (unsigned char)(value & 0xffu);
}

static void write_u64_le(unsigned char* data, uint64_t value) {
  write_u32_le(data, (uint32_t)(value & 0xffffffffu));
  write_u32_le(data + 4u, (uint32_t)((value >> 32u) & 0xffffffffu));
}

static int read_exact_test(int fd, unsigned char* data, size_t length) {
  size_t total_read = 0u;
  while (total_read < length) {
    ssize_t count = read(fd, data + total_read, length - total_read);
    if (count > 0) {
      total_read += (size_t)count;
      continue;
    }
    if (count == 0) {
      return 0;
    }
    if (errno == EINTR) {
      continue;
    }
    return 0;
  }
  return 1;
}

static int write_all_test(int fd, const unsigned char* data, size_t length) {
  size_t total_written = 0u;
  while (total_written < length) {
    ssize_t count = write(fd, data + total_written, length - total_written);
    if (count > 0) {
      total_written += (size_t)count;
      continue;
    }
    if (count == 0) {
      return 0;
    }
    if (errno == EINTR) {
      continue;
    }
    return 0;
  }
  return 1;
}

static int write_fd_frame_test(int fd, const unsigned char* payload, uint32_t payload_length) {
  unsigned char header[TEST_FRAME_HEADER_BYTES] = {};
  write_u32_be(header, payload_length);
  return write_all_test(fd, header, sizeof(header)) &&
         (payload_length == 0u || write_all_test(fd, payload, payload_length));
}

static int write_control_frame(int fd,
                               uint32_t command,
                               uint64_t sequence,
                               const unsigned char* payload,
                               uint32_t payload_length) {
  const uint32_t frame_length = TEST_CONTROL_HEADER_BYTES + payload_length;
  unsigned char* frame = NULL;
  int ok = 0;
  if (payload_length > 0u && payload == NULL) {
    return 0;
  }
  frame = (unsigned char*)calloc(1u, frame_length);
  if (frame == NULL) {
    return 0;
  }
  write_u32_le(frame, TEST_CONTROL_MAGIC);
  write_u32_le(frame + 4u, TEST_CONTROL_VERSION);
  write_u32_le(frame + 8u, command);
  write_u32_le(frame + 12u, payload_length);
  write_u64_le(frame + 16u, sequence);
  if (payload_length > 0u) {
    memcpy(frame + TEST_CONTROL_HEADER_BYTES, payload, payload_length);
  }
  ok = write_fd_frame_test(fd, frame, frame_length);
  free(frame);
  return ok;
}

static int write_simple_command(int fd, uint32_t command, uint64_t sequence) {
  return write_control_frame(fd, command, sequence, NULL, 0u);
}

static void write_uint32_field(unsigned char* payload,
                               size_t* offset,
                               uint32_t field_id,
                               uint32_t value) {
  write_u32_le(payload + *offset, field_id);
  write_u32_le(payload + *offset + 4u, TEST_CONTROL_VALUE_UINT32);
  write_u32_le(payload + *offset + 8u, 4u);
  write_u32_le(payload + *offset + 12u, value);
  *offset += 16u;
}

static void write_string_field(unsigned char* payload,
                               size_t* offset,
                               uint32_t field_id,
                               const char* value) {
  const uint32_t value_length = (uint32_t)strlen(value);
  write_u32_le(payload + *offset, field_id);
  write_u32_le(payload + *offset + 4u, TEST_CONTROL_VALUE_STRING);
  write_u32_le(payload + *offset + 8u, value_length);
  memcpy(payload + *offset + 12u, value, value_length);
  *offset += 12u + value_length;
}

static int write_read_batch_command(int fd,
                                    uint64_t sequence,
                                    uint32_t max_records,
                                    uint32_t max_bytes) {
  unsigned char payload[128] = {};
  size_t offset = 4u;
  write_u32_le(payload, 3u);
  write_string_field(payload, &offset, TEST_CONTROL_FIELD_PROVIDER_ID, TEST_PRIMARY_PROVIDER_ID);
  write_uint32_field(payload, &offset, TEST_CONTROL_FIELD_MAX_BATCH_RECORDS, max_records);
  write_uint32_field(payload, &offset, TEST_CONTROL_FIELD_MAX_BATCH_BYTES, max_bytes);
  return write_control_frame(fd,
                             TEST_CONTROL_COMMAND_READ_BATCH,
                             sequence,
                             payload,
                             (uint32_t)offset);
}

static int write_ack_command(int fd, uint64_t sequence, const char* ack_token) {
  unsigned char payload[96] = {};
  const uint32_t ack_token_length = (uint32_t)strlen(ack_token);
  size_t offset = 4u;
  if (ack_token_length == 0u) {
    return 0;
  }
  write_u32_le(payload, 2u);
  write_string_field(payload, &offset, TEST_CONTROL_FIELD_PROVIDER_ID, TEST_PRIMARY_PROVIDER_ID);
  if (offset + 12u + ack_token_length > sizeof(payload)) {
    return 0;
  }
  write_u32_le(payload + offset, TEST_CONTROL_FIELD_ACK_TOKEN);
  write_u32_le(payload + offset + 4u, TEST_CONTROL_VALUE_BYTES);
  write_u32_le(payload + offset + 8u, ack_token_length);
  memcpy(payload + offset + 12u, ack_token, ack_token_length);
  offset += 12u + ack_token_length;
  return write_control_frame(fd,
                             TEST_CONTROL_COMMAND_ACK,
                             sequence,
                             payload,
                             (uint32_t)offset);
}

static int build_log_frame(unsigned char* frame,
                           size_t frame_capacity,
                           uint64_t sequence,
                           int32_t level,
                           const char* category,
                           const char* payload,
                           size_t* out_frame_length) {
  const uint32_t category_length = (uint32_t)strlen(category);
  const uint32_t payload_text_length = (uint32_t)strlen(payload);
  const uint32_t payload_length = 4u + 16u + 12u + category_length + 12u + payload_text_length;
  size_t offset = TEST_CONTROL_HEADER_BYTES + 4u;
  if (frame_capacity < TEST_CONTROL_HEADER_BYTES + payload_length) {
    return 0;
  }
  memset(frame, 0, frame_capacity);
  write_u32_le(frame, TEST_CONTROL_MAGIC);
  write_u32_le(frame + 4u, TEST_CONTROL_VERSION);
  write_u32_le(frame + 8u, TEST_CONTROL_COMMAND_WRITE);
  write_u32_le(frame + 12u, payload_length);
  write_u64_le(frame + 16u, sequence);
  write_u32_le(frame + TEST_CONTROL_HEADER_BYTES, 3u);
  write_uint32_field(frame, &offset, TEST_CONTROL_FIELD_LOG_LEVEL, (uint32_t)level);
  write_string_field(frame, &offset, TEST_CONTROL_FIELD_LOG_CATEGORY, category);
  write_string_field(frame, &offset, TEST_CONTROL_FIELD_LOG_PAYLOAD, payload);
  *out_frame_length = TEST_CONTROL_HEADER_BYTES + payload_length;
  return 1;
}

static int write_log_command(int fd,
                             uint64_t sequence,
                             int32_t level,
                             const char* category,
                             const char* payload) {
  unsigned char frame[256] = {};
  size_t frame_length = 0u;
  return build_log_frame(frame,
                         sizeof(frame),
                         sequence,
                         level,
                         category,
                         payload,
                         &frame_length) &&
         write_fd_frame_test(fd, frame, (uint32_t)frame_length);
}

static void free_control_response(test_control_response_t* response) {
  if (response == NULL) {
    return;
  }
  free(response->body);
  memset(response, 0, sizeof(*response));
}

static int read_control_response(int fd, test_control_response_t* out_response) {
  unsigned char frame_header[TEST_FRAME_HEADER_BYTES] = {};
  unsigned char* payload = NULL;
  uint32_t payload_length = 0u;

  if (out_response == NULL ||
      !read_exact_test(fd, frame_header, sizeof(frame_header))) {
    return 0;
  }
  memset(out_response, 0, sizeof(*out_response));
  payload_length = read_u32_be(frame_header);
  if (payload_length < TEST_CONTROL_RESPONSE_HEADER_BYTES) {
    return 0;
  }
  payload = (unsigned char*)calloc(1u, payload_length);
  if (payload == NULL) {
    return 0;
  }
  if (!read_exact_test(fd, payload, payload_length)) {
    free(payload);
    return 0;
  }

  out_response->sequence = read_u64_le(payload);
  out_response->command = read_u32_le(payload + 8u);
  out_response->status = read_u32_le(payload + 12u);
  out_response->body_length = payload_length - TEST_CONTROL_RESPONSE_HEADER_BYTES;
  if (out_response->body_length > 0u) {
    out_response->body = (unsigned char*)calloc(1u, out_response->body_length);
    if (out_response->body == NULL) {
      free(payload);
      return 0;
    }
    memcpy(out_response->body,
           payload + TEST_CONTROL_RESPONSE_HEADER_BYTES,
           out_response->body_length);
  }
  free(payload);
  return 1;
}

static int assert_ok_response(int fd, uint64_t sequence, uint32_t command) {
  test_control_response_t response = {};
  if (!read_control_response(fd, &response) ||
      response.sequence != sequence ||
      response.command != command ||
      response.status != OUTBOX_STATUS_OK) {
    free_control_response(&response);
    return 0;
  }
  free_control_response(&response);
  return 1;
}

static int close_pipes_with_command(outbox_pipes_t* pipes,
                                    uint64_t sequence) {
  if (pipes == NULL) {
    return 0;
  }
  if (!write_simple_command(pipes->command_write_fd,
                            TEST_CONTROL_COMMAND_CLOSE_PIPES,
                            sequence) ||
      !assert_ok_response(pipes->record_read_fd,
                          sequence,
                          TEST_CONTROL_COMMAND_CLOSE_PIPES)) {
    return 0;
  }
  close(pipes->command_write_fd);
  close(pipes->doorbell_read_fd);
  close(pipes->record_read_fd);
  pipes->command_write_fd = -1;
  pipes->doorbell_read_fd = -1;
  pipes->record_read_fd = -1;
  return 1;
}

static int read_record_batch_response(int fd,
                                      uint64_t expected_sequence,
                                      uint32_t expected_command,
                                      uint32_t* out_status,
                                      char* ack_token,
                                      size_t ack_token_capacity,
                                      uint32_t* out_record_count,
                                      char* first_record,
                                      size_t first_record_capacity) {
  test_control_response_t response = {};
  const unsigned char* payload = NULL;
  uint32_t ack_token_length = 0u;
  uint32_t record_count = 0u;
  uint32_t offset = TEST_RECORD_RESPONSE_HEADER_BYTES;
  uint32_t first_record_length = 0u;
  int ok = 0;

  if (!read_control_response(fd, &response)) {
    return 0;
  }
  if (response.sequence != expected_sequence ||
      response.command != expected_command ||
      response.body_length < TEST_RECORD_RESPONSE_HEADER_BYTES) {
    free_control_response(&response);
    return 0;
  }
  payload = response.body;

  *out_status = response.status;
  ack_token_length = read_u32_le(payload);
  record_count = read_u32_le(payload + 4u);
  *out_record_count = record_count;
  if (ack_token_length >= ack_token_capacity ||
      offset + ack_token_length > response.body_length) {
    free_control_response(&response);
    return 0;
  }
  memcpy(ack_token, payload + offset, ack_token_length);
  ack_token[ack_token_length] = '\0';
  offset += ack_token_length;

  if (record_count > 0u) {
    if (offset + 4u > response.body_length) {
      free_control_response(&response);
      return 0;
    }
    first_record_length = read_u32_le(payload + offset);
    offset += 4u;
    if (first_record_length >= first_record_capacity ||
        offset + first_record_length > response.body_length) {
      free_control_response(&response);
      return 0;
    }
    memcpy(first_record, payload + offset, first_record_length);
    first_record[first_record_length] = '\0';
  }
  ok = 1;
  free_control_response(&response);
  return ok;
}

static int read_doorbell_event(int fd, uint32_t* out_event) {
  unsigned char header[4] = {};
  unsigned char payload[4] = {};
  ssize_t count = read(fd, header, sizeof(header));
  if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
    return 0;
  }
  if (count != (ssize_t)sizeof(header) || read_u32_be(header) != sizeof(payload)) {
    return -1;
  }
  count = read(fd, payload, sizeof(payload));
  if (count != (ssize_t)sizeof(payload)) {
    return -1;
  }
  *out_event = read_u32_le(payload);
  return 1;
}

static int drain_doorbells(int fd, uint32_t* events, size_t capacity, size_t* out_count) {
  int flags = fcntl(fd, F_GETFL, 0);
  size_t count = 0u;
  ASSERT_TRUE(flags >= 0);
  ASSERT_TRUE(fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0);
  for (;;) {
    uint32_t event = 0u;
    int result = read_doorbell_event(fd, &event);
    if (result == 0) {
      break;
    }
    ASSERT_TRUE(result > 0);
    if (count < capacity) {
      events[count] = event;
    }
    count += 1u;
  }
  *out_count = count;
  return 1;
}

static int contains_event(const uint32_t* events, size_t count, uint32_t event) {
  size_t index = 0u;
  for (index = 0u; index < count; ++index) {
    if (events[index] == event) {
      return 1;
    }
  }
  return 0;
}

static int test_unacked_batch_survives_restart(void) {
  test_context_t context = {};
  outbox_record_batch_t batch = {};
  char* first_ack = NULL;
  ASSERT_TRUE(setup_context(&context));
  ASSERT_TRUE(start_logger(&context));
  ASSERT_TRUE(log_and_flush("network.http", "first"));
  ASSERT_TRUE(log_and_flush("network.http", "second"));

  ASSERT_STATUS_OK(outbox_read_next_batch(TEST_PRIMARY_PROVIDER_ID, 1u, 4096u, &batch));
  ASSERT_TRUE(batch.record_count == 1u);
  ASSERT_TRUE(strstr(batch.records[0], "first") != NULL);
  first_ack = strdup(batch.ack_token);
  ASSERT_TRUE(first_ack != NULL);
  outbox_free_record_batch(&batch);
  ASSERT_STATUS_OK(outbox_ack(TEST_PRIMARY_PROVIDER_ID, first_ack));
  free(first_ack);

  ASSERT_STATUS_OK(outbox_read_next_batch(TEST_PRIMARY_PROVIDER_ID, 1u, 4096u, &batch));
  ASSERT_TRUE(batch.record_count == 1u);
  ASSERT_TRUE(strstr(batch.records[0], "second") != NULL);
  outbox_free_record_batch(&batch);

  outbox_stop();
  ASSERT_TRUE(start_logger(&context));
  ASSERT_STATUS_OK(outbox_read_next_batch(TEST_PRIMARY_PROVIDER_ID, 1u, 4096u, &batch));
  ASSERT_TRUE(batch.record_count == 1u);
  ASSERT_TRUE(strstr(batch.records[0], "second") != NULL);
  ASSERT_STATUS_OK(outbox_ack(TEST_PRIMARY_PROVIDER_ID, batch.ack_token));
  outbox_free_record_batch(&batch);

  ASSERT_STATUS_OK(outbox_read_next_batch(TEST_PRIMARY_PROVIDER_ID, 1u, 4096u, &batch));
  ASSERT_TRUE(batch.record_count == 0u);
  outbox_free_record_batch(&batch);
  test_report.single_primary_records_written = 2u;
  test_report.single_primary_delivered_records = 2u;
  test_report.single_primary_acked_batches = 2u;
  test_report.single_primary_retried_after_restart = 1u;
  teardown_context(&context);
  return 0;
}

static int test_open_pipes_reports_existing_backlog(void) {
  test_context_t context = {};
  outbox_pipes_t pipes = {};
  uint32_t events[4] = {};
  size_t event_count = 0u;
  ASSERT_TRUE(setup_context(&context));
  ASSERT_TRUE(start_logger(&context));
  ASSERT_TRUE(log_and_flush("network.http", "backlog"));

  ASSERT_STATUS_OK(outbox_open_pipes(&pipes));
  ASSERT_TRUE(drain_doorbells(pipes.doorbell_read_fd, events, 4u, &event_count));
  ASSERT_TRUE(contains_event(events, event_count, TEST_DOORBELL_HANDSHAKE));
  ASSERT_TRUE(contains_event(events, event_count, TEST_DOORBELL_DATA_AVAILABLE));
  ASSERT_TRUE(close_pipes_with_command(&pipes, 11u));
  teardown_context(&context);
  return 0;
}

static int test_ack_reports_remaining_backlog(void) {
  test_context_t context = {};
  outbox_pipes_t pipes = {};
  outbox_record_batch_t batch = {};
  uint32_t events[8] = {};
  size_t event_count = 0u;
  ASSERT_TRUE(setup_context(&context));
  ASSERT_TRUE(start_logger(&context));
  ASSERT_TRUE(log_and_flush("network.http", "first"));
  ASSERT_TRUE(log_and_flush("network.http", "second"));
  ASSERT_STATUS_OK(outbox_open_pipes(&pipes));
  ASSERT_TRUE(drain_doorbells(pipes.doorbell_read_fd, events, 8u, &event_count));

  ASSERT_STATUS_OK(outbox_read_next_batch(TEST_PRIMARY_PROVIDER_ID, 1u, 4096u, &batch));
  ASSERT_TRUE(batch.record_count == 1u);
  ASSERT_STATUS_OK(outbox_ack(TEST_PRIMARY_PROVIDER_ID, batch.ack_token));
  outbox_free_record_batch(&batch);

  event_count = 0u;
  ASSERT_TRUE(drain_doorbells(pipes.doorbell_read_fd, events, 8u, &event_count));
  ASSERT_TRUE(contains_event(events, event_count, TEST_DOORBELL_DATA_AVAILABLE));
  ASSERT_TRUE(close_pipes_with_command(&pipes, 12u));
  teardown_context(&context);
  return 0;
}

static int test_provider_cursors_are_independent(void) {
  test_context_t context = {};
  outbox_record_batch_t primary_batch = {};
  outbox_record_batch_t secondary_batch = {};
  ASSERT_TRUE(setup_context(&context));
  ASSERT_TRUE(start_logger(&context));
  ASSERT_TRUE(log_and_flush("network.http", "first"));
  ASSERT_TRUE(log_and_flush("network.http", "second"));

  ASSERT_STATUS_OK(outbox_read_next_batch(TEST_PRIMARY_PROVIDER_ID, 1u, 4096u, &primary_batch));
  ASSERT_TRUE(primary_batch.record_count == 1u);
  ASSERT_TRUE(strstr(primary_batch.records[0], "first") != NULL);
  ASSERT_STATUS_OK(outbox_ack(TEST_PRIMARY_PROVIDER_ID, primary_batch.ack_token));
  outbox_free_record_batch(&primary_batch);

  ASSERT_STATUS_OK(outbox_read_next_batch(TEST_SECONDARY_PROVIDER_ID, 2u, 4096u, &secondary_batch));
  ASSERT_TRUE(secondary_batch.record_count == 2u);
  ASSERT_TRUE(strstr(secondary_batch.records[0], "first") != NULL);
  ASSERT_TRUE(strstr(secondary_batch.records[1], "second") != NULL);
  ASSERT_STATUS_OK(outbox_ack(TEST_SECONDARY_PROVIDER_ID, secondary_batch.ack_token));
  outbox_free_record_batch(&secondary_batch);

  ASSERT_STATUS_OK(outbox_read_next_batch(TEST_PRIMARY_PROVIDER_ID, 1u, 4096u, &primary_batch));
  ASSERT_TRUE(primary_batch.record_count == 1u);
  ASSERT_TRUE(strstr(primary_batch.records[0], "second") != NULL);
  ASSERT_STATUS_OK(outbox_ack(TEST_PRIMARY_PROVIDER_ID, primary_batch.ack_token));
  outbox_free_record_batch(&primary_batch);

  test_report.multi_primary_delivered_records = 2u;
  test_report.multi_primary_acked_batches = 2u;
  test_report.multi_secondary_delivered_records = 2u;
  test_report.multi_secondary_acked_batches = 1u;
  test_report.multi_cursor_independence_verified = 1u;
  teardown_context(&context);
  return 0;
}

static int test_ack_requires_existing_provider_cursor(void) {
  test_context_t context = {};
  outbox_record_batch_t primary_batch = {};
  outbox_record_batch_t secondary_batch = {};
  ASSERT_TRUE(setup_context(&context));
  ASSERT_TRUE(start_logger(&context));
  ASSERT_TRUE(log_and_flush("network.http", "first"));

  ASSERT_STATUS_OK(outbox_read_next_batch(TEST_PRIMARY_PROVIDER_ID,
                                                        1u,
                                                        4096u,
                                                        &primary_batch));
  ASSERT_TRUE(primary_batch.record_count == 1u);
  ASSERT_TRUE(outbox_ack(TEST_SECONDARY_PROVIDER_ID, primary_batch.ack_token) ==
              OUTBOX_STATUS_INVALID_ARGUMENT);

  /* A provider cursor is created by readBatch, not by ack. A new provider still
   * starts from the first durable line because the rejected ACK did not move it. */
  ASSERT_STATUS_OK(outbox_read_next_batch(TEST_SECONDARY_PROVIDER_ID, 1u, 4096u, &secondary_batch));
  ASSERT_TRUE(secondary_batch.record_count == 1u);
  ASSERT_TRUE(strstr(secondary_batch.records[0], "first") != NULL);

  ASSERT_STATUS_OK(outbox_ack(TEST_PRIMARY_PROVIDER_ID, primary_batch.ack_token));
  ASSERT_STATUS_OK(outbox_ack(TEST_SECONDARY_PROVIDER_ID, secondary_batch.ack_token));
  outbox_free_record_batch(&primary_batch);
  outbox_free_record_batch(&secondary_batch);
  teardown_context(&context);
  return 0;
}

static int test_data_available_doorbell_is_coalesced_until_batch_read(void) {
  test_context_t context = {};
  outbox_pipes_t pipes = {};
  char ack_token[64] = {};
  char first_record[512] = {};
  uint32_t status = 0u;
  uint32_t record_count = 0u;
  uint32_t events[8] = {};
  size_t event_count = 0u;
  ASSERT_TRUE(setup_context(&context));
  ASSERT_TRUE(start_logger(&context));
  ASSERT_STATUS_OK(outbox_open_pipes(&pipes));
  ASSERT_TRUE(drain_doorbells(pipes.doorbell_read_fd, events, 8u, &event_count));
  ASSERT_TRUE(contains_event(events, event_count, TEST_DOORBELL_HANDSHAKE));

  ASSERT_TRUE(log_and_flush("network.http", "first"));
  ASSERT_TRUE(log_and_flush("network.http", "second"));
  ASSERT_TRUE(log_and_flush("network.http", "third"));

  event_count = 0u;
  ASSERT_TRUE(drain_doorbells(pipes.doorbell_read_fd, events, 8u, &event_count));
  ASSERT_TRUE(event_count == 1u);
  ASSERT_TRUE(events[0] == TEST_DOORBELL_DATA_AVAILABLE);

  ASSERT_TRUE(write_read_batch_command(pipes.command_write_fd, 51u, 1u, 4096u));
  ASSERT_TRUE(read_record_batch_response(pipes.record_read_fd,
                                         51u,
                                         TEST_CONTROL_COMMAND_READ_BATCH,
                                         &status,
                                         ack_token,
                                         sizeof(ack_token),
                                         &record_count,
                                         first_record,
                                         sizeof(first_record)));
  ASSERT_TRUE(status == OUTBOX_STATUS_OK);
  ASSERT_TRUE(record_count == 1u);
  ASSERT_TRUE(strstr(first_record, "first") != NULL);

  ASSERT_TRUE(write_ack_command(pipes.command_write_fd, 52u, ack_token));
  ASSERT_TRUE(assert_ok_response(pipes.record_read_fd,
                                 52u,
                                 TEST_CONTROL_COMMAND_ACK));

  event_count = 0u;
  ASSERT_TRUE(drain_doorbells(pipes.doorbell_read_fd, events, 8u, &event_count));
  ASSERT_TRUE(event_count == 1u);
  ASSERT_TRUE(events[0] == TEST_DOORBELL_DATA_AVAILABLE);

  ASSERT_TRUE(close_pipes_with_command(&pipes, 53u));
  teardown_context(&context);
  return 0;
}

static int test_control_pipe_accepts_log_command_frame(void) {
  test_context_t context = {};
  outbox_pipes_t pipes = {};
  outbox_record_batch_t batch = {};
  uint32_t events[8] = {};
  size_t event_count = 0u;
  const uint64_t sequence = 21u;
  const uint64_t flush_sequence = 22u;
  ASSERT_TRUE(setup_context(&context));
  ASSERT_TRUE(start_logger(&context));
  ASSERT_STATUS_OK(outbox_open_pipes(&pipes));
  ASSERT_TRUE(drain_doorbells(pipes.doorbell_read_fd, events, 8u, &event_count));

  ASSERT_TRUE(write_log_command(pipes.command_write_fd,
                                sequence,
                                4,
                                "network.http",
                                "pipe-frame-log"));
  ASSERT_TRUE(write_simple_command(pipes.command_write_fd,
                                   TEST_CONTROL_COMMAND_FLUSH,
                                   flush_sequence));
  ASSERT_TRUE(assert_ok_response(pipes.record_read_fd,
                                 flush_sequence,
                                 TEST_CONTROL_COMMAND_FLUSH));
  ASSERT_STATUS_OK(outbox_read_next_batch(TEST_PRIMARY_PROVIDER_ID, 1u, 4096u, &batch));
  ASSERT_TRUE(batch.record_count == 1u);
  ASSERT_TRUE(strstr(batch.records[0], "pipe-frame-log") != NULL);
  outbox_free_record_batch(&batch);

  ASSERT_TRUE(close_pipes_with_command(&pipes, 23u));
  teardown_context(&context);
  return 0;
}

static int test_control_pipe_reads_and_acks_batch_frames(void) {
  test_context_t context = {};
  outbox_pipes_t pipes = {};
  char ack_token[64] = {};
  char first_record[512] = {};
  uint32_t status = 0u;
  uint32_t record_count = 0u;
  uint32_t events[8] = {};
  size_t event_count = 0u;
  const uint64_t read_sequence = 31u;
  const uint64_t ack_sequence = 32u;
  ASSERT_TRUE(setup_context(&context));
  ASSERT_TRUE(start_logger(&context));
  ASSERT_TRUE(log_and_flush("network.http", "first"));
  ASSERT_TRUE(log_and_flush("network.http", "second"));
  ASSERT_STATUS_OK(outbox_open_pipes(&pipes));
  ASSERT_TRUE(drain_doorbells(pipes.doorbell_read_fd, events, 8u, &event_count));

  ASSERT_TRUE(write_read_batch_command(pipes.command_write_fd, read_sequence, 1u, 4096u));
  ASSERT_TRUE(read_record_batch_response(pipes.record_read_fd,
                                         read_sequence,
                                         TEST_CONTROL_COMMAND_READ_BATCH,
                                         &status,
                                         ack_token,
                                         sizeof(ack_token),
                                         &record_count,
                                         first_record,
                                         sizeof(first_record)));
  ASSERT_TRUE(status == OUTBOX_STATUS_OK);
  ASSERT_TRUE(record_count == 1u);
  ASSERT_TRUE(strstr(first_record, "first") != NULL);

  ASSERT_TRUE(write_ack_command(pipes.command_write_fd, ack_sequence, ack_token));
  ASSERT_TRUE(assert_ok_response(pipes.record_read_fd,
                                 ack_sequence,
                                 TEST_CONTROL_COMMAND_ACK));

  event_count = 0u;
  ASSERT_TRUE(drain_doorbells(pipes.doorbell_read_fd, events, 8u, &event_count));
  ASSERT_TRUE(contains_event(events, event_count, TEST_DOORBELL_DATA_AVAILABLE));

  ASSERT_TRUE(close_pipes_with_command(&pipes, 33u));
  teardown_context(&context);
  return 0;
}

static int test_control_pipe_returns_stats_response(void) {
  test_context_t context = {};
  outbox_pipes_t pipes = {};
  test_control_response_t response = {};
  uint32_t events[8] = {};
  size_t event_count = 0u;
  const uint64_t stats_sequence = 41u;
  ASSERT_TRUE(setup_context(&context));
  ASSERT_TRUE(start_logger(&context));
  ASSERT_TRUE(log_and_flush("network.http", "stats-record"));
  ASSERT_STATUS_OK(outbox_open_pipes(&pipes));
  ASSERT_TRUE(drain_doorbells(pipes.doorbell_read_fd, events, 8u, &event_count));

  ASSERT_TRUE(write_simple_command(pipes.command_write_fd,
                                   TEST_CONTROL_COMMAND_GET_STATS,
                                   stats_sequence));
  ASSERT_TRUE(read_control_response(pipes.record_read_fd, &response));
  ASSERT_TRUE(response.sequence == stats_sequence);
  ASSERT_TRUE(response.command == TEST_CONTROL_COMMAND_GET_STATS);
  ASSERT_TRUE(response.status == OUTBOX_STATUS_OK);
  ASSERT_TRUE(response.body_length == TEST_STATS_BODY_BYTES);
  ASSERT_TRUE(read_u64_le(response.body) == 1u);
  ASSERT_TRUE(read_u64_le(response.body + 40u) >= 1u);
  free_control_response(&response);

  ASSERT_TRUE(close_pipes_with_command(&pipes, 42u));
  teardown_context(&context);
  return 0;
}

static int test_segment_retention_prunes_unacked_backlog(void) {
  enum {
    RECORD_COUNT = 48,
    MAX_ARCHIVED_SEGMENTS = 1,
    MAX_SEGMENT_COUNT = MAX_ARCHIVED_SEGMENTS + 1,
  };
  test_context_t context = {};
  outbox_config_t config = {};
  outbox_record_batch_t batch = {};
  outbox_stats_t stats = {};
  uint32_t index = 0u;
  uint32_t segment_count = 0u;
  ASSERT_TRUE(setup_context(&context));

  config = config_for(&context);
  config.max_segment_size_bytes = 512u;
  config.max_archived_segments = MAX_ARCHIVED_SEGMENTS;
  ASSERT_STATUS_OK(outbox_start(&config));

  for (index = 0u; index < RECORD_COUNT; ++index) {
    char payload[256] = {};
    snprintf(payload,
             sizeof(payload),
             "retention record=%u padding=abcdefghijklmnopqrstuvwxyz0123456789"
             "abcdefghijklmnopqrstuvwxyz0123456789",
             index);
    ASSERT_STATUS_OK(outbox_log(4, "retention.cap", payload));
    ASSERT_STATUS_OK(outbox_flush());
  }

  outbox_get_stats(&stats);
  ASSERT_TRUE(stats.roll_count > MAX_SEGMENT_COUNT);
  ASSERT_TRUE(count_segment_files(context.spool_dir, &segment_count));
  ASSERT_TRUE(segment_count <= MAX_SEGMENT_COUNT);

  ASSERT_STATUS_OK(outbox_read_next_batch(TEST_PRIMARY_PROVIDER_ID, 8u, 4096u, &batch));
  ASSERT_TRUE(batch.record_count > 0u);
  outbox_free_record_batch(&batch);

  teardown_context(&context);
  return 0;
}

static int test_concurrent_producers_flush_all_records(void) {
  enum {
    WORKER_COUNT = 4,
    RECORDS_PER_WORKER = 64,
  };
  test_context_t context = {};
  outbox_config_t config = {};
  pthread_t threads[WORKER_COUNT] = {};
  concurrent_log_worker_t workers[WORKER_COUNT] = {};
  outbox_stats_t stats = {};
  uint64_t started_ns = 0u;
  uint64_t elapsed_ns = 0u;
  uint64_t queue_full_retries = 0u;
  uint32_t index = 0u;
  ASSERT_TRUE(setup_context(&context));

  config = config_for(&context);
  config.queue_capacity = 16u;
  ASSERT_STATUS_OK(outbox_start(&config));

  started_ns = monotonic_ns();
  for (index = 0u; index < WORKER_COUNT; ++index) {
    workers[index].worker_id = index;
    workers[index].record_count = RECORDS_PER_WORKER;
    ASSERT_TRUE(pthread_create(&threads[index],
                               NULL,
                               concurrent_log_worker_main,
                               &workers[index]) == 0);
  }
  for (index = 0u; index < WORKER_COUNT; ++index) {
    ASSERT_TRUE(pthread_join(threads[index], NULL) == 0);
    ASSERT_TRUE(workers[index].failed == 0);
    queue_full_retries += workers[index].queue_full_retry_count;
  }
  elapsed_ns = monotonic_ns() - started_ns;

  ASSERT_STATUS_OK(outbox_flush());
  outbox_get_stats(&stats);
  ASSERT_TRUE(stats.accepted_count == (uint64_t)WORKER_COUNT * RECORDS_PER_WORKER);
  ASSERT_TRUE(stats.written_count == (uint64_t)WORKER_COUNT * RECORDS_PER_WORKER);
  ASSERT_TRUE(stats.queue_depth == 0u);

  test_report.concurrent_worker_count = WORKER_COUNT;
  test_report.concurrent_records_per_worker = RECORDS_PER_WORKER;
  test_report.concurrent_total_records = (uint64_t)WORKER_COUNT * RECORDS_PER_WORKER;
  test_report.concurrent_queue_capacity = config.queue_capacity;
  test_report.concurrent_high_watermark = stats.queue_high_watermark;
  test_report.concurrent_queue_full_retries = queue_full_retries;
  test_report.concurrent_elapsed_ns = elapsed_ns;

  teardown_context(&context);
  return 0;
}

static int test_large_payload_round_trips_through_spool(void) {
  enum {
    PAYLOAD_BYTES = 64u * 1024u,
  };
  test_context_t context = {};
  outbox_config_t config = {};
  outbox_record_batch_t batch = {};
  char* payload = NULL;
  uint32_t index = 0u;
  ASSERT_TRUE(setup_context(&context));

  payload = (char*)calloc(1u, PAYLOAD_BYTES + 1u);
  ASSERT_TRUE(payload != NULL);
  for (index = 0u; index < PAYLOAD_BYTES; ++index) {
    payload[index] = (char)('a' + (index % 26u));
  }

  config = config_for(&context);
  config.queue_capacity = 2u;
  config.max_record_bytes = PAYLOAD_BYTES + 1u;
  config.max_segment_size_bytes = (uint64_t)PAYLOAD_BYTES * 2u;
  ASSERT_STATUS_OK(outbox_start(&config));
  ASSERT_STATUS_OK(outbox_log(4, "network.large_payload", payload));
  ASSERT_STATUS_OK(outbox_flush());

  ASSERT_STATUS_OK(outbox_read_next_batch(
      TEST_PRIMARY_PROVIDER_ID,
      1u,
      PAYLOAD_BYTES + 2048u,
      &batch));
  ASSERT_TRUE(batch.record_count == 1u);
  ASSERT_TRUE(strstr(batch.records[0], "network.large_payload") != NULL);
  ASSERT_TRUE(strlen(batch.records[0]) > PAYLOAD_BYTES);
  ASSERT_STATUS_OK(outbox_ack(TEST_PRIMARY_PROVIDER_ID, batch.ack_token));
  test_report.large_payload_bytes = PAYLOAD_BYTES;
  outbox_free_record_batch(&batch);
  free(payload);
  teardown_context(&context);
  return 0;
}

static int test_concurrent_stop_is_idempotent(void) {
  enum {
    STOP_WORKER_COUNT = 4,
  };
  test_context_t context = {};
  pthread_t threads[STOP_WORKER_COUNT] = {};
  concurrent_stop_worker_t workers[STOP_WORKER_COUNT] = {};
  outbox_stats_t stats = {};
  uint32_t index = 0u;

  ASSERT_TRUE(setup_context(&context));
  ASSERT_TRUE(start_logger(&context));
  ASSERT_TRUE(log_and_flush("lifecycle.stop", "before-stop"));

  for (index = 0u; index < STOP_WORKER_COUNT; ++index) {
    workers[index].failed = 1;
    ASSERT_TRUE(pthread_create(&threads[index],
                               NULL,
                               concurrent_stop_worker_main,
                               &workers[index]) == 0);
  }
  for (index = 0u; index < STOP_WORKER_COUNT; ++index) {
    ASSERT_TRUE(pthread_join(threads[index], NULL) == 0);
    ASSERT_TRUE(workers[index].failed == 0);
  }

  outbox_get_stats(&stats);
  ASSERT_TRUE(stats.started == 0u);
  teardown_context(&context);
  return 0;
}

int main(void) {
  ASSERT_TRUE(test_unacked_batch_survives_restart() == 0);
  ASSERT_TRUE(test_open_pipes_reports_existing_backlog() == 0);
  ASSERT_TRUE(test_ack_reports_remaining_backlog() == 0);
  ASSERT_TRUE(test_provider_cursors_are_independent() == 0);
  ASSERT_TRUE(test_ack_requires_existing_provider_cursor() == 0);
  ASSERT_TRUE(test_data_available_doorbell_is_coalesced_until_batch_read() == 0);
  ASSERT_TRUE(test_control_pipe_accepts_log_command_frame() == 0);
  ASSERT_TRUE(test_control_pipe_reads_and_acks_batch_frames() == 0);
  ASSERT_TRUE(test_control_pipe_returns_stats_response() == 0);
  ASSERT_TRUE(test_segment_retention_prunes_unacked_backlog() == 0);
  ASSERT_TRUE(test_concurrent_producers_flush_all_records() == 0);
  ASSERT_TRUE(test_large_payload_round_trips_through_spool() == 0);
  ASSERT_TRUE(test_concurrent_stop_is_idempotent() == 0);
  fprintf(stdout,
          "{\n"
          "  \"test\": \"android_outbox_core_test\",\n"
          "  \"result\": \"passed\",\n"
          "  \"kind\": \"native\",\n"
          "  \"diagnostics\": {\n"
          "    \"concurrent_producers\": {\n"
          "      \"workers\": %u,\n"
          "      \"records_per_worker\": %u,\n"
          "      \"records\": %llu,\n"
          "      \"queue\": %u,\n"
          "      \"high_watermark\": %u,\n"
          "      \"queue_full_retries\": %llu,\n"
          "      \"elapsed_ms\": %.2f,\n"
          "      \"throughput_records_per_sec\": %.2f\n"
          "    },\n"
          "    \"large_payload\": {\n"
          "      \"payload_bytes\": %u\n"
          "    }\n"
          "  },\n"
          "  \"scenarios\": [\n"
          "    {\n"
          "      \"scenario\": \"single-provider\",\n"
          "      \"records_written\": %u,\n"
          "      \"retried_after_restart\": %s,\n"
          "      \"providers\": [\n"
          "        {\n"
          "          \"provider\": \"%s\",\n"
          "          \"result\": \"passed\",\n"
          "          \"read_records\": %u,\n"
          "          \"acked_batches\": %u\n"
          "        }\n"
          "      ]\n"
          "    },\n"
          "    {\n"
          "      \"scenario\": \"multi-provider\",\n"
          "      \"cursor_independence_verified\": %s,\n"
          "      \"providers\": [\n"
          "        {\n"
          "          \"provider\": \"%s\",\n"
          "          \"result\": \"passed\",\n"
          "          \"read_records\": %u,\n"
          "          \"acked_batches\": %u\n"
          "        },\n"
          "        {\n"
          "          \"provider\": \"%s\",\n"
          "          \"result\": \"passed\",\n"
          "          \"read_records\": %u,\n"
          "          \"acked_batches\": %u\n"
          "        }\n"
          "      ]\n"
          "    }\n"
          "  ]\n"
          "}\n",
          test_report.concurrent_worker_count,
          test_report.concurrent_records_per_worker,
          (unsigned long long)test_report.concurrent_total_records,
          test_report.concurrent_queue_capacity,
          test_report.concurrent_high_watermark,
          (unsigned long long)test_report.concurrent_queue_full_retries,
          (double)test_report.concurrent_elapsed_ns / 1000000.0,
          test_report.concurrent_elapsed_ns == 0u
              ? 0.0
              : ((double)test_report.concurrent_total_records * 1000000000.0 /
                 (double)test_report.concurrent_elapsed_ns),
          test_report.large_payload_bytes,
          test_report.single_primary_records_written,
          test_report.single_primary_retried_after_restart ? "true" : "false",
          TEST_PRIMARY_PROVIDER_ID,
          test_report.single_primary_delivered_records,
          test_report.single_primary_acked_batches,
          test_report.multi_cursor_independence_verified ? "true" : "false",
          TEST_PRIMARY_PROVIDER_ID,
          test_report.multi_primary_delivered_records,
          test_report.multi_primary_acked_batches,
          TEST_SECONDARY_PROVIDER_ID,
          test_report.multi_secondary_delivered_records,
          test_report.multi_secondary_acked_batches);
  return 0;
}
