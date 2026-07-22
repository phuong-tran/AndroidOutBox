#ifndef OBSERVABILITY_LOGGER_SPOOL_H
#define OBSERVABILITY_LOGGER_SPOOL_H

#include "observability_logger_internal.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

uint64_t observability_logger_spool_file_size_bytes(const char* path);
int observability_logger_spool_ensure_directory(const char* path);
size_t observability_logger_spool_line_buffer_capacity(uint32_t max_record_bytes);
char* observability_logger_spool_segment_path(const char* directory, uint64_t segment_id);
char* observability_logger_spool_cursor_directory_path(const char* directory);
char* observability_logger_spool_provider_cursor_path(
    const char* cursor_directory,
    const char* provider_id);
int observability_logger_spool_provider_id_is_valid(const char* provider_id);
int observability_logger_spool_parse_cursor_token(
    const char* token,
    uint64_t* out_segment_id,
    uint64_t* out_offset);
int observability_logger_spool_scan_segment_bounds(
    const char* directory,
    uint64_t* out_min_segment_id,
    uint64_t* out_max_segment_id,
    uint32_t* out_segment_count);
void observability_logger_spool_load_cursor_locked(
    observability_logger_t* logger,
    observability_logger_provider_cursor_t* cursor,
    uint64_t min_segment_id,
    uint64_t segment_count);
observability_logger_provider_cursor_t* observability_logger_spool_get_provider_cursor_locked(
    observability_logger_t* logger,
    const char* provider_id,
    int create_if_missing);
int observability_logger_spool_open_segment_append_fd(const char* file_path);
int observability_logger_spool_cleanup_old_segments_locked(
    observability_logger_t* logger);
int observability_logger_spool_has_unacked_records_locked(
    const observability_logger_t* logger);
int observability_logger_spool_rotate_active_segment_locked(
    observability_logger_t* logger);

#ifdef __cplusplus
}
#endif

#endif
