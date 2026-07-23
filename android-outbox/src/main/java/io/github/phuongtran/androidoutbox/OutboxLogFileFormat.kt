package io.github.phuongtran.androidoutbox

/**
 * Stable line format written by the native outbox and consumed by Kotlin.
 *
 * Format:
 * wall_time_ms<TAB>sequence<TAB>level<TAB>category<TAB>payload<LF>
 *
 * Payload must be compact, sanitized, and single-line before it is submitted to
 * the outbox. The native layer treats payload as opaque text.
 */
object OutboxLogFileFormat {
    const val FIELD_DELIMITER = '\t'
    const val FIELD_COUNT = 5

    const val FIELD_INDEX_WALL_TIME_MS = 0
    const val FIELD_INDEX_SEQUENCE = 1
    const val FIELD_INDEX_LEVEL = 2
    const val FIELD_INDEX_CATEGORY = 3
    const val FIELD_INDEX_PAYLOAD = 4
}
