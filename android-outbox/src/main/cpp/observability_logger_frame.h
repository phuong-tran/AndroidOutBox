#ifndef OBSERVABILITY_LOGGER_FRAME_H
#define OBSERVABILITY_LOGGER_FRAME_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define OBS_LOGGER_FRAME_HEADER_BYTES 4u

/* Generic pipe frame helpers shared by the control and response lanes.
 * This layer only knows about length-prefixed bytes, fd flags, and endian
 * encoding; logger queue/spool ownership stays in observability_logger_core.c.
 */
typedef enum observability_logger_frame_read_status_t {
  OBS_LOGGER_FRAME_READ_OK = 0,
  OBS_LOGGER_FRAME_READ_WOULD_BLOCK = 1,
  OBS_LOGGER_FRAME_READ_CLOSED = 2,
  OBS_LOGGER_FRAME_READ_IO = 3,
  OBS_LOGGER_FRAME_READ_NOMEM = 4
} observability_logger_frame_read_status_t;

typedef struct observability_logger_frame_reader_t {
  int fd;
  uint32_t max_frame_bytes;
  uint8_t header[OBS_LOGGER_FRAME_HEADER_BYTES];
  uint32_t header_have;
  uint32_t frame_length;
  uint8_t* frame;
  uint32_t frame_have;
} observability_logger_frame_reader_t;

uint32_t observability_logger_read_u32_le(const uint8_t* data);
uint32_t observability_logger_read_u32_be(const uint8_t* data);
uint64_t observability_logger_read_u64_le(const uint8_t* data);
void observability_logger_write_u32_le(uint32_t value, uint8_t* data);
void observability_logger_write_u32_be(uint32_t value, uint8_t* data);
void observability_logger_write_u64_le(uint64_t value, uint8_t* data);

int observability_logger_set_fd_cloexec(int fd);
int observability_logger_set_fd_nonblocking(int fd);
int observability_logger_write_all_bytes(int fd, const void* data, size_t length);
int observability_logger_write_pipe_all_bytes(int fd, const void* data, size_t length);

observability_logger_frame_read_status_t observability_logger_wait_readable_fd(
    int fd,
    int timeout_ms);
void observability_logger_frame_reader_init(
    observability_logger_frame_reader_t* reader,
    int fd,
    uint32_t max_frame_bytes);
void observability_logger_frame_reader_reset(
    observability_logger_frame_reader_t* reader);
observability_logger_frame_read_status_t observability_logger_frame_reader_read_try(
    observability_logger_frame_reader_t* reader,
    uint8_t** out_frame,
    uint32_t* out_frame_length);

#ifdef __cplusplus
}
#endif

#endif
