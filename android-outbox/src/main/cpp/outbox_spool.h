#ifndef OUTBOX_SPOOL_H
#define OUTBOX_SPOOL_H

#include "outbox_internal.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

uint64_t outbox_spool_file_size_bytes(const char* path);
int outbox_spool_ensure_directory(const char* path);
size_t outbox_spool_line_buffer_capacity(uint32_t max_record_bytes);
char* outbox_spool_segment_path(const char* directory, uint64_t segment_id);
char* outbox_spool_cursor_directory_path(const char* directory);
char* outbox_spool_provider_cursor_path(
    const char* cursor_directory,
    const char* provider_id);
int outbox_spool_provider_id_is_valid(const char* provider_id);
int outbox_spool_parse_cursor_token(
    const char* token,
    uint64_t* out_segment_id,
    uint64_t* out_offset);
int outbox_spool_scan_segment_bounds(
    const char* directory,
    uint64_t* out_min_segment_id,
    uint64_t* out_max_segment_id,
    uint32_t* out_segment_count);
void outbox_spool_load_cursor_locked(
    outbox_t* outbox,
    outbox_provider_cursor_t* cursor,
    uint64_t min_segment_id,
    uint64_t segment_count);
outbox_provider_cursor_t* outbox_spool_get_provider_cursor_locked(
    outbox_t* outbox,
    const char* provider_id,
    int create_if_missing);
int outbox_spool_open_segment_append_fd(const char* file_path);
int outbox_spool_cleanup_old_segments_locked(
    outbox_t* outbox);
int outbox_spool_has_unacked_records_locked(
    const outbox_t* outbox);
int outbox_spool_rotate_active_segment_locked(
    outbox_t* outbox);

#ifdef __cplusplus
}
#endif

#endif
