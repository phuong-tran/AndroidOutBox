#include "observability_logger_core.h"

#include <dirent.h>
#include <errno.h>
#include <pthread.h>
#include <sched.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define DEFAULT_WORKER_COUNT 8u
#define DEFAULT_RECORDS_PER_WORKER 20000u
#define DEFAULT_QUEUE_CAPACITY 1024u
#define DEFAULT_MAX_RECORD_BYTES 512u
#define STRESS_SEGMENT_BYTES (4ull * 1024ull * 1024ull)
#define STRESS_PRIMARY_PROVIDER_ID "stress"
#define STRESS_SECONDARY_PROVIDER_ID "secondary"
#define STRESS_SAMPLE_BATCH_RECORDS 32u
#define STRESS_SAMPLE_BATCH_BYTES (64u * 1024u)

typedef struct stress_context_t {
  char spool_dir[256];
} stress_context_t;

typedef struct stress_worker_t {
  uint32_t worker_id;
  uint32_t record_count;
  uint64_t queue_full_retry_count;
  observability_logger_status_t failed_status;
} stress_worker_t;

typedef struct stress_result_t {
  observability_logger_stats_t stats;
  uint64_t queue_full_retries;
  uint64_t elapsed_ns;
  uint32_t sampled_records;
  uint32_t acked_batches;
} stress_result_t;

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

static int setup_context(stress_context_t* context) {
  snprintf(context->spool_dir, sizeof(context->spool_dir), "/tmp/obs_logger_stress_XXXXXX");
  return mkdtemp(context->spool_dir) != NULL ? 1 : 0;
}

static void teardown_context(stress_context_t* context) {
  observability_logger_close_pipes();
  observability_logger_stop();
  remove_tree(context->spool_dir);
}

static uint32_t parse_u32_arg(const char* text, uint32_t fallback) {
  char* end = NULL;
  unsigned long value = 0u;
  if (text == NULL || text[0] == '\0') {
    return fallback;
  }
  errno = 0;
  value = strtoul(text, &end, 10);
  if (errno != 0 || end == text || *end != '\0' || value == 0u || value > UINT32_MAX) {
    return fallback;
  }
  return (uint32_t)value;
}

static uint64_t monotonic_ns(void) {
  struct timespec ts = {};
  if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
    return 0u;
  }
  return ((uint64_t)ts.tv_sec * 1000000000ull) + (uint64_t)ts.tv_nsec;
}

static void* stress_worker_main(void* opaque) {
  stress_worker_t* worker = (stress_worker_t*)opaque;
  uint32_t index = 0u;

  for (index = 0u; index < worker->record_count; ++index) {
    char payload[192] = {};
    observability_logger_status_t status;
    snprintf(payload,
             sizeof(payload),
             "worker=%u record=%u source=native_stress kind=mpsc payload=abcdefghijklmnopqrstuvwxyz",
             worker->worker_id,
             index);
    /* Stress keeps pressure on the MPSC ring without treating backpressure as a
     * failure. Queue-full retries are reported so we can see producer speed
     * versus the single native file writer. */
    do {
      status = observability_logger_log(4, "stress.mpsc", payload);
      if (status == OBSERVABILITY_LOGGER_STATUS_QUEUE_FULL) {
        worker->queue_full_retry_count += 1u;
        sched_yield();
      }
    } while (status == OBSERVABILITY_LOGGER_STATUS_QUEUE_FULL);

    if (status != OBSERVABILITY_LOGGER_STATUS_OK) {
      worker->failed_status = status;
      return NULL;
    }
  }
  return NULL;
}

/*
 * After the producer/writer stress path has already been measured, read a small
 * sample through one or two providers. This does not participate in throughput;
 * it only proves the same retained spool can be consumed by independent sink
 * cursors without making the stress output noisy.
 */
static int read_and_ack_single_consumer_sample(stress_result_t* result) {
  observability_logger_record_batch_t batch = {};
  if (observability_logger_read_next_batch(STRESS_PRIMARY_PROVIDER_ID,
                                           STRESS_SAMPLE_BATCH_RECORDS,
                                           STRESS_SAMPLE_BATCH_BYTES,
                                           &batch) != OBSERVABILITY_LOGGER_STATUS_OK) {
    return 0;
  }
  if (batch.record_count == 0u ||
      observability_logger_ack(STRESS_PRIMARY_PROVIDER_ID, batch.ack_token) !=
          OBSERVABILITY_LOGGER_STATUS_OK) {
    observability_logger_free_record_batch(&batch);
    return 0;
  }
  result->sampled_records = batch.record_count;
  result->acked_batches = 1u;
  observability_logger_free_record_batch(&batch);
  return 1;
}

static int read_and_ack_multi_consumer_sample(stress_result_t* result) {
  observability_logger_record_batch_t primary_batch = {};
  observability_logger_record_batch_t secondary_batch = {};
  char* first_primary_record = NULL;
  int success = 0;

  if (observability_logger_read_next_batch(STRESS_PRIMARY_PROVIDER_ID,
                                           STRESS_SAMPLE_BATCH_RECORDS,
                                           STRESS_SAMPLE_BATCH_BYTES,
                                           &primary_batch) != OBSERVABILITY_LOGGER_STATUS_OK ||
      primary_batch.record_count == 0u) {
    goto cleanup;
  }
  first_primary_record = strdup(primary_batch.records[0]);
  if (first_primary_record == NULL ||
      observability_logger_ack(STRESS_PRIMARY_PROVIDER_ID, primary_batch.ack_token) !=
          OBSERVABILITY_LOGGER_STATUS_OK) {
    goto cleanup;
  }

  if (observability_logger_read_next_batch(STRESS_SECONDARY_PROVIDER_ID,
                                           STRESS_SAMPLE_BATCH_RECORDS,
                                           STRESS_SAMPLE_BATCH_BYTES,
                                           &secondary_batch) != OBSERVABILITY_LOGGER_STATUS_OK ||
      secondary_batch.record_count != primary_batch.record_count ||
      strcmp(secondary_batch.records[0], first_primary_record) != 0 ||
      observability_logger_ack(STRESS_SECONDARY_PROVIDER_ID, secondary_batch.ack_token) !=
          OBSERVABILITY_LOGGER_STATUS_OK) {
    goto cleanup;
  }

  result->sampled_records = secondary_batch.record_count;
  result->acked_batches = 2u;
  success = 1;

cleanup:
  free(first_primary_record);
  observability_logger_free_record_batch(&primary_batch);
  observability_logger_free_record_batch(&secondary_batch);
  return success;
}

static void print_stress_report(uint32_t worker_count,
                                uint64_t total_records,
                                uint32_t queue_capacity,
                                const stress_result_t* single_consumer_result,
                                const stress_result_t* multi_consumer_result) {
  /* Pretty JSON is emitted once after both scenarios complete for human review
   * and copy/paste diagnostics. It is intentionally kept outside the hot path. */
  fprintf(stdout,
          "{\n"
          "  \"test\": \"android_outbox_core_stress_test\",\n"
          "  \"result\": \"passed\",\n"
          "  \"kind\": \"native-stress\",\n"
          "  \"scenarios\": [\n"
          "    {\n"
          "      \"scenario\": \"single-consumer\",\n"
          "      \"consumers\": \"%s\",\n"
          "      \"sources\": \"stress.mpsc\",\n"
          "      \"workers\": %u,\n"
          "      \"records\": %llu,\n"
          "      \"queue\": %u,\n"
          "      \"high_watermark\": %u,\n"
          "      \"queue_full_retries\": %llu,\n"
          "      \"sampled_records\": %u,\n"
          "      \"acked_batches\": %u,\n"
          "      \"elapsed_ms\": %.2f,\n"
          "      \"throughput_records_per_sec\": %.2f\n"
          "    },\n"
          "    {\n"
          "      \"scenario\": \"multi-consumer\",\n"
          "      \"consumers\": \"%s\",\n"
          "      \"sources\": \"stress.mpsc\",\n"
          "      \"workers\": %u,\n"
          "      \"records\": %llu,\n"
          "      \"queue\": %u,\n"
          "      \"high_watermark\": %u,\n"
          "      \"queue_full_retries\": %llu,\n"
          "      \"sampled_records\": %u,\n"
          "      \"acked_batches\": %u,\n"
          "      \"elapsed_ms\": %.2f,\n"
          "      \"throughput_records_per_sec\": %.2f\n"
          "    }\n"
          "  ]\n"
          "}\n",
          STRESS_PRIMARY_PROVIDER_ID,
          worker_count,
          (unsigned long long)total_records,
          queue_capacity,
          single_consumer_result->stats.queue_high_watermark,
          (unsigned long long)single_consumer_result->queue_full_retries,
          single_consumer_result->sampled_records,
          single_consumer_result->acked_batches,
          (double)single_consumer_result->elapsed_ns / 1000000.0,
          single_consumer_result->elapsed_ns == 0u
              ? 0.0
              : ((double)total_records * 1000000000.0 / (double)single_consumer_result->elapsed_ns),
          STRESS_PRIMARY_PROVIDER_ID "," STRESS_SECONDARY_PROVIDER_ID,
          worker_count,
          (unsigned long long)total_records,
          queue_capacity,
          multi_consumer_result->stats.queue_high_watermark,
          (unsigned long long)multi_consumer_result->queue_full_retries,
          multi_consumer_result->sampled_records,
          multi_consumer_result->acked_batches,
          (double)multi_consumer_result->elapsed_ns / 1000000.0,
          multi_consumer_result->elapsed_ns == 0u
              ? 0.0
              : ((double)total_records * 1000000000.0 / (double)multi_consumer_result->elapsed_ns));
}

static int run_stress_scenario(int multi_consumer,
                               uint32_t worker_count,
                               uint32_t records_per_worker,
                               uint32_t queue_capacity,
                               uint32_t max_record_bytes,
                               stress_result_t* output_result) {
  stress_context_t context = {};
  observability_logger_config_t config = {};
  stress_result_t result = {};
  pthread_t* threads = NULL;
  stress_worker_t* workers = NULL;
  uint64_t total_records = (uint64_t)worker_count * (uint64_t)records_per_worker;
  uint64_t started_ns = 0u;
  uint32_t index = 0u;
  int exit_code = 1;

  threads = (pthread_t*)calloc(worker_count, sizeof(*threads));
  workers = (stress_worker_t*)calloc(worker_count, sizeof(*workers));
  if (threads == NULL || workers == NULL || !setup_context(&context)) {
    goto cleanup;
  }

  config.spool_directory_path = context.spool_dir;
  config.default_provider_id = STRESS_PRIMARY_PROVIDER_ID;
  config.queue_capacity = queue_capacity;
  config.max_record_bytes = max_record_bytes;
  config.max_segment_size_bytes = STRESS_SEGMENT_BYTES;
  config.max_archived_segments = 128u;
  if (observability_logger_start(&config) != OBSERVABILITY_LOGGER_STATUS_OK) {
    goto cleanup;
  }

  started_ns = monotonic_ns();
  for (index = 0u; index < worker_count; ++index) {
    workers[index].worker_id = index;
    workers[index].record_count = records_per_worker;
    if (pthread_create(&threads[index], NULL, stress_worker_main, &workers[index]) != 0) {
      goto cleanup;
    }
  }
  for (index = 0u; index < worker_count; ++index) {
    if (pthread_join(threads[index], NULL) != 0 || workers[index].failed_status != 0) {
      goto cleanup;
    }
    result.queue_full_retries += workers[index].queue_full_retry_count;
  }

  if (observability_logger_flush() != OBSERVABILITY_LOGGER_STATUS_OK) {
    goto cleanup;
  }
  result.elapsed_ns = monotonic_ns() - started_ns;
  observability_logger_get_stats(&result.stats);

  if (result.stats.accepted_count != total_records ||
      result.stats.written_count != total_records ||
      result.stats.queue_depth != 0u ||
      result.stats.write_failure_count != 0u) {
    fprintf(stderr,
            "stress failed: accepted=%llu written=%llu depth=%u write_failures=%llu expected=%llu\n",
            (unsigned long long)result.stats.accepted_count,
            (unsigned long long)result.stats.written_count,
            result.stats.queue_depth,
            (unsigned long long)result.stats.write_failure_count,
            (unsigned long long)total_records);
    goto cleanup;
  }

  if (multi_consumer) {
    if (!read_and_ack_multi_consumer_sample(&result)) {
      goto cleanup;
    }
  } else {
    if (!read_and_ack_single_consumer_sample(&result)) {
      goto cleanup;
    }
  }
  *output_result = result;
  exit_code = 0;

cleanup:
  teardown_context(&context);
  free(threads);
  free(workers);
  return exit_code;
}

int main(int argc, char** argv) {
  stress_result_t single_consumer_result = {};
  stress_result_t multi_consumer_result = {};
  uint32_t worker_count = DEFAULT_WORKER_COUNT;
  uint32_t records_per_worker = DEFAULT_RECORDS_PER_WORKER;
  uint32_t queue_capacity = DEFAULT_QUEUE_CAPACITY;
  uint32_t max_record_bytes = DEFAULT_MAX_RECORD_BYTES;
  uint64_t total_records = 0u;

  if (argc > 1 && strcmp(argv[1], "--help") == 0) {
    fprintf(stdout,
            "usage: %s [workers] [records_per_worker] [queue_capacity] [max_record_bytes]\n",
            argv[0]);
    return 0;
  }

  worker_count = argc > 1 ? parse_u32_arg(argv[1], worker_count) : worker_count;
  records_per_worker = argc > 2 ? parse_u32_arg(argv[2], records_per_worker) : records_per_worker;
  queue_capacity = argc > 3 ? parse_u32_arg(argv[3], queue_capacity) : queue_capacity;
  max_record_bytes = argc > 4 ? parse_u32_arg(argv[4], max_record_bytes) : max_record_bytes;
  total_records = (uint64_t)worker_count * (uint64_t)records_per_worker;

  if (run_stress_scenario(0,
                          worker_count,
                          records_per_worker,
                          queue_capacity,
                          max_record_bytes,
                          &single_consumer_result) != 0) {
    return 1;
  }
  if (run_stress_scenario(1,
                          worker_count,
                          records_per_worker,
                          queue_capacity,
                          max_record_bytes,
                          &multi_consumer_result) != 0) {
    return 1;
  }
  print_stress_report(worker_count,
                      total_records,
                      queue_capacity,
                      &single_consumer_result,
                      &multi_consumer_result);
  return 0;
}
