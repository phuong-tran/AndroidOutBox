#include "observability_logger_core.h"

#include <jni.h>
#include <unistd.h>

namespace {

constexpr jint kPipeCount = 3;

}  // namespace

extern "C" JNIEXPORT jintArray JNICALL
Java_io_github_phuongtran_androidoutbox_NativeAndroidOutbox_nativeOpenPipes(JNIEnv* env,
                                                                                   jobject) {
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
    close(pipes.command_write_fd);
    close(pipes.doorbell_read_fd);
    close(pipes.record_read_fd);
    observability_logger_close_pipes();
    return nullptr;
  }
  env->SetIntArrayRegion(result, 0, kPipeCount, values);
  if (env->ExceptionCheck()) {
    close(pipes.command_write_fd);
    close(pipes.doorbell_read_fd);
    close(pipes.record_read_fd);
    observability_logger_close_pipes();
    return nullptr;
  }
  return result;
}
