package io.github.phuongtran.androidoutbox

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

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

    @Test(timeout = 5_000L)
    fun `one-way record frame does not wait behind a blocking control response`() {
        val transport = BlockingControlTransport()
        val client = OutboxNativeControlClient(transport)
        assertTrue(client.configure(config(maxRecordBytes = 4 * 1024)))

        val executor = Executors.newFixedThreadPool(2)
        try {
            val flush = executor.submit<Boolean> { client.flush() }
            assertTrue(transport.blockingReadStarted.await(1, TimeUnit.SECONDS))

            val write = executor.submit<Boolean> {
                client.write(
                    level = OutboxRecordLevel.ERROR,
                    category = "network.failure",
                    payload = "control lane is blocked",
                )
            }
            assertTrue(write.get(1, TimeUnit.SECONDS))
            assertEquals(3, transport.writtenCommandCount.get())

            transport.releaseBlockingRead.countDown()
            assertTrue(flush.get(1, TimeUnit.SECONDS))
        } finally {
            transport.releaseBlockingRead.countDown()
            executor.shutdownNow()
            client.close()
        }
    }

    @Test
    fun `rejects batch request above allocation limits before writing command`() {
        val transport = RecordingTransport()
        transport.enqueueOk(
            sequence = 1L,
            command = OutboxControlCommandEncoder.COMMAND_CONFIGURE,
        )
        val client = OutboxNativeControlClient(transport)
        assertTrue(client.configure(config(maxRecordBytes = 4 * 1024)))

        assertNull(
            client.readNextBatch(
                providerId = "primary",
                maxRecords = OutboxRecordStore.MAX_BATCH_RECORDS + 1,
                maxBytes = OutboxRecordStore.DEFAULT_MAX_BYTES,
            ),
        )
        assertNull(
            client.readNextBatch(
                providerId = "primary",
                maxRecords = OutboxRecordStore.DEFAULT_MAX_RECORDS,
                maxBytes = OutboxRecordStore.MAX_BATCH_BYTES + 1,
            ),
        )
        assertEquals(1, transport.writtenCommandCount)
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

    private class BlockingControlTransport : OutboxPipeTransport {
        val blockingReadStarted = CountDownLatch(1)
        val releaseBlockingRead = CountDownLatch(1)
        val writtenCommandCount = AtomicInteger(0)
        private val readCount = AtomicInteger(0)

        override fun writeCommandEnvelope(envelope: ByteBuffer): Boolean {
            writtenCommandCount.incrementAndGet()
            return true
        }

        override fun readDoorbellFrame(): ByteArray? = null

        override fun readControlResponseFrame(): ByteArray {
            return when (readCount.incrementAndGet()) {
                1 -> responseFrame(
                    sequence = 1L,
                    command = OutboxControlCommandEncoder.COMMAND_CONFIGURE,
                )
                else -> {
                    blockingReadStarted.countDown()
                    releaseBlockingRead.await(2, TimeUnit.SECONDS)
                    responseFrame(
                        sequence = 2L,
                        command = OutboxControlCommandEncoder.COMMAND_FLUSH,
                    )
                }
            }
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
