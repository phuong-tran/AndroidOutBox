package io.github.phuongtran.androidoutbox

import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * Base class for app-owned sink runners.
 *
 * Applications extend this class and implement [send]. The runner owns the
 * read/send/ACK sequence for one provider id, so competing doorbell, app-start,
 * connectivity, or manual retry triggers cannot drain the same cursor in
 * parallel.
 *
 * AndroidOutBox stays backend-agnostic. The provider id is an opaque cursor
 * name, and [send] decides whether records go to a remote endpoint, a local
 * diagnostic target, or any other app-owned destination.
 *
 * @param outbox AndroidOutBox instance used for batch reads and ACKs.
 * @param providerId Opaque provider cursor owned by this runner.
 * @param maxRecords Maximum records to fetch per batch.
 * @param maxBytes Maximum payload bytes to fetch per batch.
 * @param maxBatchesPerDrain Upper bound for one drain pass.
 */
abstract class AndroidOutboxSinkRunner(
    private val outbox: AndroidOutbox,
    val providerId: String = OutboxConfig.DEFAULT_PROVIDER_ID,
    private val maxRecords: Int = OutboxRecordStore.DEFAULT_MAX_RECORDS,
    private val maxBytes: Int = OutboxRecordStore.DEFAULT_MAX_BYTES,
    private val maxBatchesPerDrain: Int = DEFAULT_MAX_BATCHES_PER_DRAIN,
) {
    private val drainMutex = Mutex()

    init {
        require(providerId.isNotBlank()) {
            "providerId must not be blank"
        }
        require(maxRecords > 0) {
            "maxRecords must be greater than zero"
        }
        require(maxBytes > 0) {
            "maxBytes must be greater than zero"
        }
        require(maxBatchesPerDrain > 0) {
            "maxBatchesPerDrain must be greater than zero"
        }
    }

    /**
     * Runs an initial drain, then drains again whenever a matching doorbell
     * arrives. This function is long-lived and should be launched by the app in
     * an application-owned coroutine scope.
     */
    suspend fun run(doorbells: OutboxDoorbellChannel) {
        drainAvailable()
        doorbells
            .events()
            .filter(::shouldDrain)
            .collect {
                drainAvailable()
            }
    }

    /**
     * Drains retained batches for this provider.
     *
     * Calls are serialized by [drainMutex]. This preserves provider cursor
     * ordering even when multiple app triggers call this method at the same
     * time.
     */
    suspend fun drainAvailable(): OutboxDrainResult {
        return drainMutex.withLock {
            drainAvailableLocked()
        }
    }

    /**
     * Converts a doorbell into a drain trigger.
     *
     * Override this if a sink wants to react to extra events such as pressure
     * notifications. The default only drains when native reports data.
     */
    protected open fun shouldDrain(event: OutboxDoorbellEvent): Boolean {
        return event == OutboxDoorbellEvent.DATA_AVAILABLE
    }

    /**
     * Sends one batch to the destination owned by the app.
     *
     * Return true only after the destination accepts the whole batch. Returning
     * false stops the drain without ACK, so the same provider cursor can retry
     * the batch later.
     */
    protected abstract suspend fun send(records: List<String>): Boolean

    private suspend fun drainAvailableLocked(): OutboxDrainResult {
        var readBatches = 0
        var sentRecords = 0
        var ackedBatches = 0
        var stoppedByFailure = false

        while (readBatches < maxBatchesPerDrain) {
            val batch = outbox.readNextBatch(
                providerId = providerId,
                maxRecords = maxRecords,
                maxBytes = maxBytes,
            ) ?: break
            readBatches += 1

            val sent = send(batch.records)
            if (!sent) {
                stoppedByFailure = true
                break
            }
            sentRecords += batch.records.size

            val acked = outbox.ack(
                providerId = providerId,
                ackToken = batch.ackToken,
            )
            if (!acked) {
                stoppedByFailure = true
                break
            }
            ackedBatches += 1
        }

        return OutboxDrainResult(
            readBatches = readBatches,
            sentRecords = sentRecords,
            ackedBatches = ackedBatches,
            stoppedByFailure = stoppedByFailure,
        )
    }

    companion object {
        const val DEFAULT_MAX_BATCHES_PER_DRAIN = 16
    }
}
