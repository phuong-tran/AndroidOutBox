package io.github.phuongtran.androidoutbox

import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Test
import java.nio.ByteBuffer
import java.nio.ByteOrder

class OutboxControlCommandEncoderTest {
    @Test
    fun `encodes configure command as versioned little endian frame`() {
        val frame = OutboxControlCommandEncoder.configure(
            sequence = 7L,
            config = OutboxConfig(
                spoolDirectoryPath = MEMORY_ONLY_SPOOL_PATH,
                defaultProviderId = TEST_PROVIDER_ID,
                queueCapacity = 8,
                maxRecordBytes = 512,
                maxSegmentSizeBytes = 1_024L,
                maxArchivedSegments = 2,
            ),
        )
        val buffer = frame.readBuffer()

        assertEquals(0x5A4F4253, buffer.int)
        assertEquals(1, buffer.int)
        assertEquals(1, buffer.int)
        assertEquals(frame.remaining() - HEADER_BYTES, buffer.int)
        assertEquals(7L, buffer.long)
        assertEquals(6, buffer.int)

        assertEquals(1, buffer.int)
        assertEquals(1, buffer.int)
        val pathLength = buffer.int
        val pathBytes = ByteArray(pathLength)
        buffer.get(pathBytes)
        assertEquals(MEMORY_ONLY_SPOOL_PATH, String(pathBytes, Charsets.UTF_8))
        assertStringField(buffer = buffer, id = 12, value = TEST_PROVIDER_ID)
    }

    @Test
    fun `encodes flush command without payload`() {
        val frame = OutboxControlCommandEncoder.flush(sequence = 9L)
        val buffer = frame.readBuffer()

        assertEquals(0x5A4F4253, buffer.int)
        assertEquals(1, buffer.int)
        assertEquals(2, buffer.int)
        assertEquals(0, buffer.int)
        assertEquals(9L, buffer.long)
    }

    @Test
    fun `encodes read batch as command frame`() {
        val frame = OutboxControlCommandEncoder.readBatch(
            sequence = 11L,
            providerId = TEST_PROVIDER_ID,
            maxRecords = 32,
            maxBytes = 64 * 1024,
        )
        val buffer = frame.readBuffer()

        assertCommandHeader(
            buffer = buffer,
            command = 4,
            payloadLength = frame.remaining() - HEADER_BYTES,
            sequence = 11L,
        )
        assertEquals(3, buffer.int)
        assertStringField(buffer = buffer, id = 12, value = TEST_PROVIDER_ID)
        assertUint32Field(buffer = buffer, id = 6, value = 32)
        assertUint32Field(buffer = buffer, id = 7, value = 64 * 1024)
    }

    @Test
    fun `encodes ack token as bytes command frame`() {
        val ackToken = "00000000000000000001:00000000000000000128".toByteArray()
        val frame = OutboxControlCommandEncoder.ack(
            sequence = 12L,
            providerId = TEST_PROVIDER_ID,
            ackToken = ackToken,
        )
        val buffer = frame.readBuffer()

        assertCommandHeader(
            buffer = buffer,
            command = 5,
            payloadLength = frame.remaining() - HEADER_BYTES,
            sequence = 12L,
        )
        assertEquals(2, buffer.int)
        assertStringField(buffer = buffer, id = 12, value = TEST_PROVIDER_ID)
        assertEquals(8, buffer.int)
        assertEquals(4, buffer.int)
        val valueLength = buffer.int
        val value = ByteArray(valueLength)
        buffer.get(value)
        assertArrayEquals(ackToken, value)
    }

    @Test
    fun `encodes log as command frame`() {
        val frame = OutboxControlCommandEncoder.write(
            sequence = 0L,
            level = OutboxRecordLevel.ERROR,
            category = "network.http",
            payload = """{"status":500}""",
        )
        val buffer = frame.readBuffer()

        assertCommandHeader(
            buffer = buffer,
            command = 6,
            payloadLength = frame.remaining() - HEADER_BYTES,
            sequence = 0L,
        )
        assertEquals(3, buffer.int)
        assertUint32Field(buffer = buffer, id = 9, value = OutboxRecordLevel.ERROR.nativeValue)
        assertStringField(buffer = buffer, id = 10, value = "network.http")
        assertStringField(buffer = buffer, id = 11, value = """{"status":500}""")
    }

    @Test
    fun `encodes get stats command without payload`() {
        val frame = OutboxControlCommandEncoder.getStats(sequence = 14L)
        val buffer = frame.readBuffer()

        assertCommandHeader(
            buffer = buffer,
            command = 7,
            payloadLength = 0,
            sequence = 14L,
        )
    }

    @Test
    fun `encodes close pipes command without payload`() {
        val frame = OutboxControlCommandEncoder.closePipes(sequence = 15L)
        val buffer = frame.readBuffer()

        assertCommandHeader(
            buffer = buffer,
            command = 8,
            payloadLength = 0,
            sequence = 15L,
        )
    }

    @Test
    fun `command frame is a byte buffer ready for pipe write`() {
        val frame = OutboxControlCommandEncoder.stop(sequence = 13L)
        val beforePosition = frame.position()

        val firstRead = frame.readBuffer()
        val secondRead = frame.readBuffer()

        assertEquals(0, beforePosition)
        assertEquals(0, frame.position())
        assertEquals(HEADER_BYTES, frame.remaining())
        assertEquals(firstRead.int, secondRead.int)
    }

    private fun ByteBuffer.readBuffer(): ByteBuffer {
        return duplicate().order(ByteOrder.LITTLE_ENDIAN)
    }

    private fun assertCommandHeader(
        buffer: ByteBuffer,
        command: Int,
        payloadLength: Int,
        sequence: Long,
    ) {
        assertEquals(0x5A4F4253, buffer.int)
        assertEquals(1, buffer.int)
        assertEquals(command, buffer.int)
        assertEquals(payloadLength, buffer.int)
        assertEquals(sequence, buffer.long)
    }

    private fun assertUint32Field(
        buffer: ByteBuffer,
        id: Int,
        value: Int,
    ) {
        assertEquals(id, buffer.int)
        assertEquals(2, buffer.int)
        assertEquals(Int.SIZE_BYTES, buffer.int)
        assertEquals(value, buffer.int)
    }

    private fun assertStringField(
        buffer: ByteBuffer,
        id: Int,
        value: String,
    ) {
        assertEquals(id, buffer.int)
        assertEquals(1, buffer.int)
        val valueLength = buffer.int
        val valueBytes = ByteArray(valueLength)
        buffer.get(valueBytes)
        assertEquals(value, valueBytes.toString(Charsets.UTF_8))
    }

    private companion object {
        const val HEADER_BYTES = 24
        const val MEMORY_ONLY_SPOOL_PATH = "memory://observability"
        const val TEST_PROVIDER_ID = "primary"
    }
}
