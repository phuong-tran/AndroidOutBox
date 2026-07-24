@file:Suppress("TestFileNamingAndPackage")

package io.github.phuongtran.androidoutbox

import org.junit.Assert.assertTrue
import org.junit.Assume.assumeTrue
import org.junit.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

/**
 * Opt-in lifecycle race diagnostic for the host JNI path.
 *
 * This test is intentionally separate from [OutboxHostJniIntegrationTest]. The
 * integration test protects normal protocol behavior, while this class creates
 * teardown contention and should only run when explicitly requested.
 */
class OutboxHostJniShutdownRaceTest {

    @Test(timeout = TEST_TIMEOUT_MS)
    fun `host jni client should tolerate concurrent calls during shutdown`() {
        assumeTrue(isRaceTestEnabled)

        withHostClient { client, spoolDirectory ->
            configureHostOutbox(
                client = client,
                spoolDirectory = spoolDirectory,
                defaultProviderId = PRIMARY_PROVIDER_ID,
            )

            val race = ShutdownRace(client)

            race.start()
            Thread.sleep(SHUTDOWN_RACE_WINDOW_MS)
            client.stopNativeOutbox()
            client.closeNativePipes()
            client.close()
            race.stop()

            race.assertNoWorkerFailure()
        }
    }

    private class ShutdownRace(
        private val client: OutboxNativeControlClient,
    ) {
        private val startGate = CountDownLatch(1)
        private val doneGate = CountDownLatch(SHUTDOWN_WORKER_COUNT)
        private val running = AtomicBoolean(true)
        private val failure = AtomicReference<Throwable?>()
        private val executor = Executors.newFixedThreadPool(SHUTDOWN_WORKER_COUNT)

        init {
            repeat(SHUTDOWN_WORKER_COUNT) { workerIndex ->
                executor.execute {
                    runWorker(workerIndex)
                }
            }
        }

        fun start() {
            startGate.countDown()
        }

        fun stop() {
            running.set(false)
        }

        fun assertNoWorkerFailure() {
            assertTrue(doneGate.await(SHUTDOWN_WORKER_TIMEOUT_SECONDS, TimeUnit.SECONDS))
            executor.shutdownNow()
            failure.get()?.let { throwable ->
                throw AssertionError("Concurrent shutdown should not throw", throwable)
            }
        }

        private fun runWorker(workerIndex: Int) {
            runCatching {
                startGate.await()
                runWorkerLoop(workerIndex)
            }.onFailure { throwable ->
                failure.compareAndSet(null, throwable)
            }
            doneGate.countDown()
        }

        private fun runWorkerLoop(workerIndex: Int) {
            var recordIndex = 0
            while (running.get() && recordIndex < SHUTDOWN_MAX_WRITES_PER_WORKER) {
                client.write(
                    level = OutboxRecordLevel.ERROR,
                    category = "shutdown.race.$workerIndex",
                    payload = """{"record":$recordIndex}""",
                )
                maybeFlush(recordIndex)
                maybeReadAndAck(
                    workerIndex = workerIndex,
                    recordIndex = recordIndex,
                )
                maybeReadStats(recordIndex)
                recordIndex += 1
            }
        }

        private fun maybeFlush(recordIndex: Int) {
            if (recordIndex % SHUTDOWN_FLUSH_INTERVAL == 0) {
                client.flush()
            }
        }

        private fun maybeReadAndAck(
            workerIndex: Int,
            recordIndex: Int,
        ) {
            if (recordIndex % SHUTDOWN_READ_INTERVAL != 0) {
                return
            }
            val providerId = "shutdown-$workerIndex"
            val batch = client.readNextBatch(
                providerId = providerId,
                maxRecords = 2,
                maxBytes = 4096,
            ) ?: return
            client.ack(
                providerId = providerId,
                ackToken = batch.ackToken,
            )
        }

        private fun maybeReadStats(recordIndex: Int) {
            if (recordIndex % SHUTDOWN_STATS_INTERVAL == 0) {
                client.getStats()
            }
        }
    }

    private companion object {
        val isRaceTestEnabled: Boolean
            get() = System.getProperty("androidoutbox.hostJniRace") == "true"

        const val TEST_TIMEOUT_MS = 10_000L
        const val PRIMARY_PROVIDER_ID = "primary"
        const val SHUTDOWN_WORKER_COUNT = 4
        const val SHUTDOWN_MAX_WRITES_PER_WORKER = 128
        const val SHUTDOWN_FLUSH_INTERVAL = 8
        const val SHUTDOWN_READ_INTERVAL = 16
        const val SHUTDOWN_STATS_INTERVAL = 32
        const val SHUTDOWN_RACE_WINDOW_MS = 25L
        const val SHUTDOWN_WORKER_TIMEOUT_SECONDS = 5L
    }
}
