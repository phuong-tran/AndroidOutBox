package io.github.phuongtran.androidoutbox

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.ArrayDeque
import java.util.concurrent.atomic.AtomicInteger

class AndroidOutboxSinkRunnerTest {

    @Test
    fun `drain sends and ACKs batches in order`() = runBlocking {
        val outbox = FakeAndroidOutbox(
            batches = listOf(
                batch(token = "1", records = listOf("first")),
                batch(token = "2", records = listOf("second", "third")),
            ),
        )
        val runner = FakeSinkRunner(
            outbox = outbox,
            sendResults = arrayDequeOf(true, true),
        )

        val result = runner.drainAvailable()

        assertEquals(2, result.readBatches)
        assertEquals(3, result.sentRecords)
        assertEquals(2, result.ackedBatches)
        assertFalse(result.stoppedByFailure)
        assertEquals(listOf("1", "2"), outbox.ackedTokens)
        assertEquals(
            listOf(
                listOf("first"),
                listOf("second", "third"),
            ),
            runner.sentBatches,
        )
    }

    @Test
    fun `drain stops without ACK when sink fails`() = runBlocking {
        val outbox = FakeAndroidOutbox(
            batches = listOf(
                batch(token = "1", records = listOf("first")),
                batch(token = "2", records = listOf("second")),
            ),
        )
        val runner = FakeSinkRunner(
            outbox = outbox,
            sendResults = arrayDequeOf(false),
        )

        val result = runner.drainAvailable()

        assertEquals(1, result.readBatches)
        assertEquals(0, result.sentRecords)
        assertEquals(0, result.ackedBatches)
        assertTrue(result.stoppedByFailure)
        assertEquals(emptyList<String>(), outbox.ackedTokens)
        assertEquals(listOf(listOf("first")), runner.sentBatches)
    }

    @Test
    fun `factory creates lambda backed sink runner`() = runBlocking {
        val sentBatches = mutableListOf<List<String>>()
        val outbox = FakeAndroidOutbox(
            batches = listOf(
                batch(token = "1", records = listOf("first")),
            ),
        )
        val runner = AndroidOutboxSinkRunner(
            outbox = outbox,
            providerId = "primary",
        ) { records ->
            sentBatches += records
            true
        }

        val result = runner.drainAvailable()

        assertEquals(1, result.readBatches)
        assertEquals(1, result.sentRecords)
        assertEquals(1, result.ackedBatches)
        assertFalse(result.stoppedByFailure)
        assertEquals(listOf("1"), outbox.ackedTokens)
        assertEquals(listOf(listOf("first")), sentBatches)
    }

    @Test
    fun `concurrent drain calls are serialized for one provider cursor`() = runBlocking {
        val firstSendStarted = CompletableDeferred<Unit>()
        val releaseFirstSend = CompletableDeferred<Unit>()
        val outbox = FakeAndroidOutbox(
            batches = listOf(
                batch(token = "1", records = listOf("first")),
                batch(token = "2", records = listOf("second")),
            ),
        )
        val runner = object : AndroidOutboxSinkRunner(outbox) {
            private val sendCount = AtomicInteger()

            override suspend fun send(records: List<String>): Boolean {
                if (sendCount.incrementAndGet() == 1) {
                    firstSendStarted.complete(Unit)
                    releaseFirstSend.await()
                }
                return true
            }
        }

        val firstDrain = async(Dispatchers.Default) {
            runner.drainAvailable()
        }
        firstSendStarted.await()

        val secondDrain = async(Dispatchers.Default) {
            runner.drainAvailable()
        }
        delay(CONCURRENT_DRAIN_ASSERTION_DELAY_MS)

        assertEquals(1, outbox.readCount.get())
        releaseFirstSend.complete(Unit)

        firstDrain.await()
        secondDrain.await()

        assertEquals(listOf("1", "2"), outbox.ackedTokens)
    }

    private class FakeSinkRunner(
        outbox: AndroidOutbox,
        private val sendResults: ArrayDeque<Boolean>,
    ) : AndroidOutboxSinkRunner(outbox) {
        val sentBatches = mutableListOf<List<String>>()

        override suspend fun send(records: List<String>): Boolean {
            sentBatches += records
            return sendResults.removeFirst()
        }
    }

    private class FakeAndroidOutbox(
        batches: List<OutboxBatch>,
    ) : AndroidOutbox {
        private val pendingBatches = ArrayDeque(batches)
        val ackedTokens = mutableListOf<String>()
        val readCount = AtomicInteger()

        override fun start(config: OutboxConfig): Boolean = true

        override fun write(
            level: OutboxRecordLevel,
            category: String,
            payload: String,
        ): Boolean = true

        override fun flush(): Boolean = true

        override fun stop() = Unit

        override fun getStats(): OutboxStats = OutboxStats()

        override fun readNextDoorbell(): OutboxDoorbellEvent? {
            return OutboxDoorbellEvent.DATA_AVAILABLE
        }

        override fun readNextBatch(
            providerId: String,
            maxRecords: Int,
            maxBytes: Int,
        ): OutboxBatch? {
            readCount.incrementAndGet()
            return pendingBatches.peekFirst()
        }

        override fun ack(
            providerId: String,
            ackToken: ByteArray,
        ): Boolean {
            val token = ackToken.decodeToString()
            val pending = pendingBatches.peekFirst() ?: return false
            if (!pending.ackToken.contentEquals(ackToken)) {
                return false
            }
            pendingBatches.removeFirst()
            ackedTokens += token
            return true
        }
    }

    private companion object {
        const val CONCURRENT_DRAIN_ASSERTION_DELAY_MS = 50L

        fun batch(
            token: String,
            records: List<String>,
        ): OutboxBatch {
            return OutboxBatch(
                ackToken = token.toByteArray(),
                records = records,
            )
        }

        fun arrayDequeOf(vararg values: Boolean): ArrayDeque<Boolean> {
            return ArrayDeque<Boolean>().apply {
                values.forEach(::add)
            }
        }
    }
}
