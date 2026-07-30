package io.github.phuongtran.androidoutbox

/**
 * Lightweight file-first outbox for app-owned records.
 *
 * This API is intentionally vendor-agnostic. Callers must pass compact,
 * sanitized, single-line payloads; remote uploaders or local consumers live
 * above this layer.
 */
interface AndroidOutbox : OutboxDoorbellReader, OutboxRecordStore {
    fun start(config: OutboxConfig): Boolean

    /**
     * Hands a sanitized payload to native for best-effort local persistence.
     *
     * Returns true after Kotlin writes a complete command frame to the native
     * pipe. Native queue pressure is reported asynchronously through
     * counters/doorbells, so this result is not a durable-acceptance signal.
     */
    fun write(
        level: OutboxRecordLevel,
        category: String,
        payload: String,
    ): Boolean

    /**
     * Waits until records already accepted by the native writer have been
     * drained from the in-memory queue and written to the active segment file.
     */
    fun flush(): Boolean

    /**
     * Flushes accepted records, then requests the OS to sync the active segment
     * file to stable storage. This is optional and intentionally separate from
     * [flush].
     */
    fun forceSync(): Boolean

    fun stop()

    fun getStats(): OutboxStats
}
