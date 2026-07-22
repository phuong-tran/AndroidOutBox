package io.github.phuongtran.androidoutbox

/**
 * Native-to-Kotlin wake-up signal.
 *
 * Doorbells intentionally carry no log payload. Kotlin uses the event to wake
 * a consumer, then reads durable records from the spool/cursor layer.
 */
enum class OutboxDoorbellEvent(
    private val nativeValue: Int,
) {
    HANDSHAKE(0),
    DATA_AVAILABLE(1),
    DROPPED_RECORD(2),
    UNKNOWN(-1),
    ;

    companion object {
        fun fromNativeValue(nativeValue: Int): OutboxDoorbellEvent {
            return entries.firstOrNull { event ->
                event.nativeValue == nativeValue
            } ?: UNKNOWN
        }
    }
}
