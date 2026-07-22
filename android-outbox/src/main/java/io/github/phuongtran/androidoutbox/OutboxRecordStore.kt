package io.github.phuongtran.androidoutbox

/**
 * Cursor-facing reader for durable outbox records.
 *
 * Native owns the committed cursor. Kotlin reads a batch, attempts transport,
 * and acknowledges the opaque token only after transport success. Failed
 * transport means no ACK, so the same records are returned on the next read.
 */
interface OutboxRecordStore {
    fun readNextBatch(
        providerId: String = OutboxConfig.DEFAULT_PROVIDER_ID,
        maxRecords: Int = DEFAULT_MAX_RECORDS,
        maxBytes: Int = DEFAULT_MAX_BYTES,
    ): OutboxBatch?

    fun ack(
        providerId: String = OutboxConfig.DEFAULT_PROVIDER_ID,
        ackToken: ByteArray,
    ): Boolean

    companion object {
        const val DEFAULT_MAX_RECORDS = 32
        const val DEFAULT_MAX_BYTES = 64 * 1024
    }
}
