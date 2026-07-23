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
     * Enqueues a sanitized payload for local persistence.
     *
     * Returns false when the outbox is unavailable or cannot hand the frame to
     * native. Native queue pressure is reported through counters/doorbells so
     * write failure never affects the caller flow.
     */
    fun write(
        level: OutboxRecordLevel,
        category: String,
        payload: String,
    ): Boolean

    /**
     * Waits until records already accepted by the native writer have been
     * drained from the in-memory queue and written to the active segment file.
     *
     * This is a local writer barrier, not a database-grade durability promise.
     */
    fun flush(): Boolean

    fun stop()

    fun getStats(): OutboxStats
}
