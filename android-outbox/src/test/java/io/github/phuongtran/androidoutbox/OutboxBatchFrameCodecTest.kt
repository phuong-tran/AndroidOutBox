package io.github.phuongtran.androidoutbox

import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test
import java.nio.ByteBuffer
import java.nio.ByteOrder

class OutboxBatchFrameCodecTest {
    @Test
    fun `decodes successful record batch response`() {
        val ackToken = "00000000000000000001:00000000000000000128".toByteArray()
        val records = listOf(
            "record-one",
            "record-two",
        )

        val batch = OutboxBatchFrameCodec.decode(
            recordBatchBody(
                ackToken = ackToken,
                records = records,
            ),
        )

        requireNotNull(batch)
        assertArrayEquals(ackToken, batch.ackToken)
        assertEquals(records, batch.records)
    }

    @Test
    fun `returns null for empty batch body`() {
        val batch = OutboxBatchFrameCodec.decode(
            recordBatchBody(
                ackToken = ByteArray(0),
                records = emptyList(),
            ),
        )

        assertNull(batch)
    }

    @Test
    fun `returns null for truncated response`() {
        val batch = OutboxBatchFrameCodec.decode(byteArrayOf(0, 0, 0, 0))

        assertNull(batch)
    }

    private fun recordBatchBody(
        ackToken: ByteArray,
        records: List<String>,
    ): ByteArray {
        val recordBytes = records.map { record -> record.toByteArray(Charsets.UTF_8) }
        val payloadSize = HEADER_BYTES +
            ackToken.size +
            recordBytes.sumOf { record -> Int.SIZE_BYTES + record.size }
        val buffer = ByteBuffer
            .allocate(payloadSize)
            .order(ByteOrder.LITTLE_ENDIAN)
        buffer.putInt(ackToken.size)
        buffer.putInt(records.size)
        buffer.put(ackToken)
        recordBytes.forEach { record ->
            buffer.putInt(record.size)
            buffer.put(record)
        }
        return buffer.array()
    }

    private companion object {
        const val HEADER_BYTES = 8
    }
}
