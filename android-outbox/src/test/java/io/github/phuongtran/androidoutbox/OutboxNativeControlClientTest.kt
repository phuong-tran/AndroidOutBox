package io.github.phuongtran.androidoutbox

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import java.nio.ByteBuffer
import java.nio.ByteOrder

class OutboxNativeControlClientTest {
    @Test
    fun `rejects huge payload before writing a command frame`() {
        val transport = RecordingTransport()
        transport.enqueueOk(
            sequence = 1L,
            command = OutboxControlCommandEncoder.COMMAND_CONFIGURE,
        )
        val client = OutboxNativeControlClient(transport)

        assertTrue(client.configure(config(maxRecordBytes = 4 * 1024)))

        val accepted = client.write(
            level = OutboxRecordLevel.ERROR,
            category = "network.failure",
            payload = "x".repeat(2 * 1024 * 1024),
        )

        assertFalse(accepted)
        assertEquals(1, transport.writtenCommandCount)
    }

    @Test
    fun `rejects payload whose utf8 byte count reaches max record bytes`() {
        val transport = RecordingTransport()
        transport.enqueueOk(
            sequence = 1L,
            command = OutboxControlCommandEncoder.COMMAND_CONFIGURE,
        )
        val client = OutboxNativeControlClient(transport)

        assertTrue(client.configure(config(maxRecordBytes = 6)))

        assertFalse(
            client.write(
                level = OutboxRecordLevel.ERROR,
                category = "network.failure",
                payload = "ééé",
            ),
        )
        assertEquals(1, transport.writtenCommandCount)
    }

    @Test
    fun `rejects oversized category before writing a command frame`() {
        val transport = RecordingTransport()
        transport.enqueueOk(
            sequence = 1L,
            command = OutboxControlCommandEncoder.COMMAND_CONFIGURE,
        )
        val client = OutboxNativeControlClient(transport)

        assertTrue(client.configure(config(maxRecordBytes = 4 * 1024)))

        assertFalse(
            client.write(
                level = OutboxRecordLevel.ERROR,
                category = "c".repeat(OutboxConfig.MAX_CATEGORY_BYTES + 1),
                payload = """{"ok":true}""",
            ),
        )
        assertEquals(1, transport.writtenCommandCount)
    }

    @Test
    fun `stats include Kotlin side preflight rejections`() {
        val transport = RecordingTransport()
        transport.enqueueOk(
            sequence = 1L,
            command = OutboxControlCommandEncoder.COMMAND_CONFIGURE,
        )
        transport.enqueueOk(
            sequence = 2L,
            command = OutboxControlCommandEncoder.COMMAND_GET_STATS,
            body = statsBody(isStarted = true),
        )
        val client = OutboxNativeControlClient(transport)

        assertTrue(client.configure(config(maxRecordBytes = 4 * 1024)))
        assertFalse(
            client.write(
                level = OutboxRecordLevel.ERROR,
                category = "network.failure",
                payload = "x".repeat(2 * 1024 * 1024),
            ),
        )

        val stats = client.getStats()

        assertTrue(stats.isStarted)
        assertEquals(1L, stats.droppedRecordTooLargeCount)
        assertEquals(2, transport.writtenCommandCount)
    }

    @Test
    fun `accepted payload writes one command frame`() {
        val transport = RecordingTransport()
        transport.enqueueOk(
            sequence = 1L,
            command = OutboxControlCommandEncoder.COMMAND_CONFIGURE,
        )
        val client = OutboxNativeControlClient(transport)

        assertTrue(client.configure(config(maxRecordBytes = 4 * 1024)))
        assertTrue(
            client.write(
                level = OutboxRecordLevel.ERROR,
                category = "network.failure",
                payload = """{"status":500}""",
            ),
        )

        assertEquals(2, transport.writtenCommandCount)
    }

    private fun config(maxRecordBytes: Int): OutboxConfig {
        return OutboxConfig(
            spoolDirectoryPath = "memory://client-test",
            maxRecordBytes = maxRecordBytes,
        )
    }

    private fun statsBody(isStarted: Boolean): ByteArray {
        return ByteBuffer
            .allocate(STATS_SIZE * Long.SIZE_BYTES)
            .order(ByteOrder.LITTLE_ENDIAN)
            .putLong(if (isStarted) 1L else 0L)
            .putLong(0L)
            .putLong(0L)
            .putLong(0L)
            .putLong(0L)
            .putLong(0L)
            .putLong(0L)
            .putLong(0L)
            .putLong(0L)
            .putLong(0L)
            .putLong(0L)
            .putLong(0L)
            .putLong(0L)
            .array()
    }

    private class RecordingTransport : OutboxPipeTransport {
        private val responses = ArrayDeque<ByteArray>()
        var writtenCommandCount = 0
            private set

        fun enqueueOk(
            sequence: Long,
            command: Int,
            body: ByteArray = ByteArray(0),
        ) {
            responses += responseFrame(
                sequence = sequence,
                command = command,
                body = body,
            )
        }

        override fun writeCommandEnvelope(envelope: ByteBuffer): Boolean {
            writtenCommandCount += 1
            return true
        }

        override fun readDoorbellFrame(): ByteArray? = null

        override fun readControlResponseFrame(): ByteArray? {
            return responses.removeFirstOrNull()
        }

        override fun close() = Unit
    }

    private companion object {
        const val RESPONSE_HEADER_BYTES = 16
        const val STATS_SIZE = 13

        fun responseFrame(
            sequence: Long,
            command: Int,
            status: Int = OutboxControlResponseFrame.STATUS_OK,
            body: ByteArray = ByteArray(0),
        ): ByteArray {
            return ByteBuffer
                .allocate(RESPONSE_HEADER_BYTES + body.size)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putLong(sequence)
                .putInt(command)
                .putInt(status)
                .put(body)
                .array()
        }
    }
}
