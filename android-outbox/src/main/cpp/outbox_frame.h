#ifndef OUTBOX_FRAME_H
#define OUTBOX_FRAME_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define OUTBOX_FRAME_HEADER_BYTES 4u

/* Generic pipe frame helpers shared by the control and response lanes.
 * This layer only knows about length-prefixed bytes, fd flags, and endian
 * encoding; outbox queue/spool ownership stays in outbox_core.c.
 */
typedef enum outbox_frame_read_status_t {
  OUTBOX_FRAME_READ_OK = 0,
  OUTBOX_FRAME_READ_WOULD_BLOCK = 1,
  OUTBOX_FRAME_READ_CLOSED = 2,
  OUTBOX_FRAME_READ_IO = 3,
  OUTBOX_FRAME_READ_NOMEM = 4
} outbox_frame_read_status_t;

typedef struct outbox_frame_reader_t {
  int fd;
  uint32_t max_frame_bytes;
  uint8_t header[OUTBOX_FRAME_HEADER_BYTES];
  uint32_t header_have;
  uint32_t frame_length;
  uint8_t* frame;
  uint32_t frame_have;
} outbox_frame_reader_t;

uint32_t outbox_read_u32_le(const uint8_t* data);
uint32_t outbox_read_u32_be(const uint8_t* data);
uint64_t outbox_read_u64_le(const uint8_t* data);
void outbox_write_u32_le(uint32_t value, uint8_t* data);
void outbox_write_u32_be(uint32_t value, uint8_t* data);
void outbox_write_u64_le(uint64_t value, uint8_t* data);

int outbox_set_fd_cloexec(int fd);
int outbox_set_fd_nonblocking(int fd);
int outbox_write_all_bytes(int fd, const void* data, size_t length);
int outbox_write_pipe_all_bytes(int fd, const void* data, size_t length);

outbox_frame_read_status_t outbox_wait_readable_fd(
    int fd,
    int timeout_ms);
void outbox_frame_reader_init(
    outbox_frame_reader_t* reader,
    int fd,
    uint32_t max_frame_bytes);
void outbox_frame_reader_reset(
    outbox_frame_reader_t* reader);
outbox_frame_read_status_t outbox_frame_reader_read_try(
    outbox_frame_reader_t* reader,
    uint8_t** out_frame,
    uint32_t* out_frame_length);

#ifdef __cplusplus
}
#endif

#endif
