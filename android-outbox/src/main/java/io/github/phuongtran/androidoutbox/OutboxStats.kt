package io.github.phuongtran.androidoutbox

/**
 * Snapshot of native outbox health counters.
 *
 * These values are diagnostics for the outbox runtime itself, not business
 * payload metadata. They help Kotlin detect queue pressure, writer failures,
 * and retention behavior without inspecting native files directly.
 *
 * @property isStarted Whether the native outbox has accepted its runtime
 * configuration and started writer ownership.
 * @property queueCapacity Number of records the native MPSC queue can hold.
 * @property queueDepth Approximate number of records waiting in memory.
 * @property queueHighWatermark Highest observed queue depth since start.
 * @property nextSequence Next native sequence number to assign.
 * @property acceptedCount Number of records accepted into the native queue.
 * @property writtenCount Number of records persisted to the spool.
 * @property droppedQueueFullCount Number of records dropped because the native
 * queue was full.
 * @property droppedInvalidCount Number of invalid records rejected before
 * enqueueing.
 * @property droppedRecordTooLargeCount Number of records rejected because the
 * payload exceeded the configured record size.
 * @property writeFailureCount Number of spool write or formatting failures.
 * @property currentFileSizeBytes Current active segment size in bytes.
 * @property rollCount Number of active segment rolls performed by native.
 */
data class OutboxStats(
    val isStarted: Boolean = false,
    val queueCapacity: Int = 0,
    val queueDepth: Int = 0,
    val queueHighWatermark: Int = 0,
    val nextSequence: Long = 0L,
    val acceptedCount: Long = 0L,
    val writtenCount: Long = 0L,
    val droppedQueueFullCount: Long = 0L,
    val droppedInvalidCount: Long = 0L,
    val droppedRecordTooLargeCount: Long = 0L,
    val writeFailureCount: Long = 0L,
    val currentFileSizeBytes: Long = 0L,
    val rollCount: Long = 0L,
)
