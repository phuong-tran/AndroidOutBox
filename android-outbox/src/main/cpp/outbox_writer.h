#ifndef OUTBOX_WRITER_H
#define OUTBOX_WRITER_H

#include "outbox_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

void* outbox_writer_main(void* opaque);

#if defined(OUTBOX_TESTING)
void outbox_test_fail_next_record_append(void);
#endif

#ifdef __cplusplus
}
#endif

#endif
