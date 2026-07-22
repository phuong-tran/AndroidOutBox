#ifndef OBSERVABILITY_LOGGER_CONTROL_H
#define OBSERVABILITY_LOGGER_CONTROL_H

#ifdef __cplusplus
extern "C" {
#endif

void observability_logger_control_notify_if_unacked_records_available(void);
void* observability_logger_control_main(void* opaque);

#ifdef __cplusplus
}
#endif

#endif
