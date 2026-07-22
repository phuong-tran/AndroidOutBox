package io.github.phuongtran.androidoutbox

import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * Decodes the record batch body returned inside a native command response.
 *
 * JNI stays a thin fd bridge. Kotlin owns decoding so the native boundary does
 * not construct Java object graphs for each record.
 */
internal object OutboxBatchFrameCodec {
    fun decode(body: ByteArray): OutboxBatch? {
        val buffer = ByteBuffer
            .wrap(body)
            .order(ByteOrder.LITTLE_ENDIAN)
        if (buffer.remaining() < HEADER_BYTES) {
            return null
        }

        val ackTokenLength = buffer.int
        val recordCount = buffer.int
        if (
            ackTokenLength <= 0 ||
            recordCount <= 0 ||
            ackTokenLength > buffer.remaining()
        ) {
            return null
        }

        val ackToken = ByteArray(ackTokenLength)
        buffer.get(ackToken)
        val records = ArrayList<String>(recordCount)
        repeat(recordCount) {
            if (buffer.remaining() < RECORD_LENGTH_BYTES) {
                return null
            }
            val recordLength = buffer.int
            if (recordLength <= 0 || recordLength > buffer.remaining()) {
                return null
            }
            val record = ByteArray(recordLength)
            buffer.get(record)
            records.add(record.toString(Charsets.UTF_8))
        }
        if (buffer.hasRemaining()) {
            return null
        }
        return OutboxBatch(
            ackToken = ackToken,
            records = records,
        )
    }

    private const val HEADER_BYTES = 8
    private const val RECORD_LENGTH_BYTES = 4
}
