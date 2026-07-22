package io.github.phuongtran.androidoutbox

/**
 * Severity value persisted with each record.
 *
 * Native values are stable ABI values for the C layer, not backend-specific
 * levels. A later Kotlin consumer can map these values to any transport consumer.
 */
enum class OutboxRecordLevel(
    internal val nativeValue: Int,
) {
    TRACE(NATIVE_TRACE),
    DEBUG(NATIVE_DEBUG),
    INFO(NATIVE_INFO),
    WARN(NATIVE_WARN),
    ERROR(NATIVE_ERROR),
    FATAL(NATIVE_FATAL),
}

private const val NATIVE_TRACE = 0
private const val NATIVE_DEBUG = 1
private const val NATIVE_INFO = 2
private const val NATIVE_WARN = 3
private const val NATIVE_ERROR = 4
private const val NATIVE_FATAL = 5
