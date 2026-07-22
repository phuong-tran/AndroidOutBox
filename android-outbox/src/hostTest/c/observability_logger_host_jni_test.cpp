#include "observability_logger_core.h"

#include <jni.h>
#include <unistd.h>

namespace {

constexpr jint kPipeCount = 3;

void close_pipes(int command_write_fd, int doorbell_read_fd, int record_read_fd) {
  close(command_write_fd);
  close(doorbell_read_fd);
  close(record_read_fd);
  observability_logger_close_pipes();
}

}  // namespace

extern "C" JNIEXPORT jintArray JNICALL
Java_io_github_phuongtran_androidoutbox_HostOutboxNativeBridge_nativeOpenPipes(
    JNIEnv* env,
    jclass) {
  observability_logger_pipes_t pipes = {};
  jint values[kPipeCount] = {};
  if (observability_logger_open_pipes(&pipes) !=
      OBSERVABILITY_LOGGER_STATUS_OK) {
    return nullptr;
  }
  values[0] = pipes.command_write_fd;
  values[1] = pipes.doorbell_read_fd;
  values[2] = pipes.record_read_fd;

  jintArray result = env->NewIntArray(kPipeCount);
  if (result == nullptr) {
    close_pipes(pipes.command_write_fd, pipes.doorbell_read_fd, pipes.record_read_fd);
    return nullptr;
  }
  env->SetIntArrayRegion(result, 0, kPipeCount, values);
  if (env->ExceptionCheck()) {
    close_pipes(pipes.command_write_fd, pipes.doorbell_read_fd, pipes.record_read_fd);
    return nullptr;
  }
  return result;
}
