#include "outbox_frame.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

uint32_t outbox_read_u32_le(const uint8_t* data) {
  return (uint32_t)data[0] | ((uint32_t)data[1] << 8u) | ((uint32_t)data[2] << 16u) |
         ((uint32_t)data[3] << 24u);
}

uint32_t outbox_read_u32_be(const uint8_t* data) {
  return ((uint32_t)data[0] << 24u) | ((uint32_t)data[1] << 16u) |
         ((uint32_t)data[2] << 8u) | (uint32_t)data[3];
}

uint64_t outbox_read_u64_le(const uint8_t* data) {
  return (uint64_t)outbox_read_u32_le(data) |
         ((uint64_t)outbox_read_u32_le(data + 4u) << 32u);
}

void outbox_write_u32_le(uint32_t value, uint8_t* data) {
  data[0] = (uint8_t)(value & 0xffu);
  data[1] = (uint8_t)((value >> 8u) & 0xffu);
  data[2] = (uint8_t)((value >> 16u) & 0xffu);
  data[3] = (uint8_t)((value >> 24u) & 0xffu);
}

void outbox_write_u64_le(uint64_t value, uint8_t* data) {
  outbox_write_u32_le((uint32_t)(value & 0xffffffffu), data);
  outbox_write_u32_le((uint32_t)((value >> 32u) & 0xffffffffu),
                                    data + 4u);
}

void outbox_write_u32_be(uint32_t value, uint8_t* data) {
  data[0] = (uint8_t)((value >> 24u) & 0xffu);
  data[1] = (uint8_t)((value >> 16u) & 0xffu);
  data[2] = (uint8_t)((value >> 8u) & 0xffu);
  data[3] = (uint8_t)(value & 0xffu);
}

int outbox_set_fd_cloexec(int fd) {
  int flags = fcntl(fd, F_GETFD, 0);
  if (flags < 0) {
    return 0;
  }
  return fcntl(fd, F_SETFD, flags | FD_CLOEXEC) == 0 ? 1 : 0;
}

int outbox_set_fd_nonblocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0) {
    return 0;
  }
  return fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0 ? 1 : 0;
}

int outbox_write_all_bytes(int fd, const void* data, size_t length) {
  size_t total_written = 0u;
  const uint8_t* bytes = (const uint8_t*)data;
  while (total_written < length) {
    ssize_t written = write(fd, bytes + total_written, length - total_written);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      return 0;
    }
    if (written == 0) {
      return 0;
    }
    total_written += (size_t)written;
  }
  return 1;
}

static ssize_t write_no_sigpipe_once(int fd, const void* data, size_t length) {
#if defined(F_SETNOSIGPIPE)
  (void)fcntl(fd, F_SETNOSIGPIPE, 1);
  return write(fd, data, length);
#elif defined(SIGPIPE)
  sigset_t block_set;
  sigset_t old_set;
  sigset_t pending_set;
  int signal_masked = 0;
  int sigpipe_already_pending = 0;
  ssize_t written = 0;
  int write_errno = 0;

  sigemptyset(&block_set);
  sigaddset(&block_set, SIGPIPE);
  if (pthread_sigmask(SIG_BLOCK, &block_set, &old_set) == 0) {
    signal_masked = 1;
    if (sigismember(&old_set, SIGPIPE) == 1 &&
        sigpending(&pending_set) == 0 &&
        sigismember(&pending_set, SIGPIPE) == 1) {
      sigpipe_already_pending = 1;
    }
  }

  written = write(fd, data, length);
  write_errno = errno;

  if (signal_masked != 0) {
    if (written < 0 && write_errno == EPIPE && sigpipe_already_pending == 0) {
      const int sigpipe_pending_after_write =
          sigpending(&pending_set) == 0 && sigismember(&pending_set, SIGPIPE) == 1;
      if (sigpipe_pending_after_write != 0) {
        int caught_signal = 0;
        while (sigwait(&block_set, &caught_signal) == EINTR) {
        }
      }
    }
    pthread_sigmask(SIG_SETMASK, &old_set, NULL);
  }

  errno = write_errno;
  return written;
#else
  return write(fd, data, length);
#endif
}

int outbox_write_pipe_all_bytes(int fd, const void* data, size_t length) {
  size_t total_written = 0u;
  const uint8_t* bytes = (const uint8_t*)data;
  while (total_written < length) {
    ssize_t written = write_no_sigpipe_once(fd, bytes + total_written, length - total_written);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      return 0;
    }
    if (written == 0) {
      return 0;
    }
    total_written += (size_t)written;
  }
  return 1;
}

outbox_frame_read_status_t outbox_wait_readable_fd(
    int fd,
    int timeout_ms) {
  struct pollfd poll_fd = {};
  int rc = 0;

  poll_fd.fd = fd;
  poll_fd.events = POLLIN;
  do {
    rc = poll(&poll_fd, 1u, timeout_ms);
  } while (rc < 0 && errno == EINTR);

  if (rc == 0) {
    return OUTBOX_FRAME_READ_WOULD_BLOCK;
  }
  if (rc < 0) {
    return OUTBOX_FRAME_READ_IO;
  }
  if ((poll_fd.revents & POLLIN) != 0) {
    return OUTBOX_FRAME_READ_OK;
  }
  if ((poll_fd.revents & (POLLHUP | POLLERR | POLLNVAL)) != 0) {
    return OUTBOX_FRAME_READ_CLOSED;
  }
  return OUTBOX_FRAME_READ_WOULD_BLOCK;
}

void outbox_frame_reader_init(
    outbox_frame_reader_t* reader,
    int fd,
    uint32_t max_frame_bytes) {
  if (reader == NULL) {
    return;
  }
  memset(reader, 0, sizeof(*reader));
  reader->fd = fd;
  reader->max_frame_bytes = max_frame_bytes;
}

void outbox_frame_reader_reset(
    outbox_frame_reader_t* reader) {
  if (reader == NULL) {
    return;
  }
  free(reader->frame);
  reader->frame = NULL;
  reader->header_have = 0u;
  reader->frame_length = 0u;
  reader->frame_have = 0u;
}

static outbox_frame_read_status_t read_nonblocking_fd(
    int fd,
    uint8_t* data,
    uint32_t length,
    uint32_t* out_bytes_read) {
  ssize_t bytes_read = 0;

  if (data == NULL || out_bytes_read == NULL || length == 0u) {
    return OUTBOX_FRAME_READ_IO;
  }
  *out_bytes_read = 0u;
  bytes_read = read(fd, data, length);
  if (bytes_read > 0) {
    *out_bytes_read = (uint32_t)bytes_read;
    return OUTBOX_FRAME_READ_OK;
  }
  if (bytes_read == 0) {
    return OUTBOX_FRAME_READ_CLOSED;
  }
  if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
    return OUTBOX_FRAME_READ_WOULD_BLOCK;
  }
  return OUTBOX_FRAME_READ_IO;
}

outbox_frame_read_status_t outbox_frame_reader_read_try(
    outbox_frame_reader_t* reader,
    uint8_t** out_frame,
    uint32_t* out_frame_length) {
  if (reader == NULL || out_frame == NULL || out_frame_length == NULL) {
    return OUTBOX_FRAME_READ_IO;
  }
  *out_frame = NULL;
  *out_frame_length = 0u;

  while (reader->header_have < OUTBOX_FRAME_HEADER_BYTES) {
    uint32_t bytes_read = 0u;
    outbox_frame_read_status_t status =
        read_nonblocking_fd(reader->fd,
                            reader->header + reader->header_have,
                            OUTBOX_FRAME_HEADER_BYTES - reader->header_have,
                            &bytes_read);
    if (status == OUTBOX_FRAME_READ_WOULD_BLOCK) {
      return status;
    }
    if (status != OUTBOX_FRAME_READ_OK) {
      outbox_frame_reader_reset(reader);
      return status;
    }
    reader->header_have += bytes_read;
  }

  if (reader->frame == NULL) {
    reader->frame_length = outbox_read_u32_be(reader->header);
    if (reader->frame_length == 0u || reader->frame_length > reader->max_frame_bytes) {
      outbox_frame_reader_reset(reader);
      return OUTBOX_FRAME_READ_IO;
    }
    reader->frame = (uint8_t*)calloc(1u, reader->frame_length);
    if (reader->frame == NULL) {
      outbox_frame_reader_reset(reader);
      return OUTBOX_FRAME_READ_NOMEM;
    }
  }

  while (reader->frame_have < reader->frame_length) {
    uint32_t bytes_read = 0u;
    outbox_frame_read_status_t status =
        read_nonblocking_fd(reader->fd,
                            reader->frame + reader->frame_have,
                            reader->frame_length - reader->frame_have,
                            &bytes_read);
    if (status == OUTBOX_FRAME_READ_WOULD_BLOCK) {
      return status;
    }
    if (status != OUTBOX_FRAME_READ_OK) {
      outbox_frame_reader_reset(reader);
      return status;
    }
    reader->frame_have += bytes_read;
  }

  *out_frame = reader->frame;
  *out_frame_length = reader->frame_length;
  reader->frame = NULL;
  reader->header_have = 0u;
  reader->frame_length = 0u;
  reader->frame_have = 0u;
  return OUTBOX_FRAME_READ_OK;
}
