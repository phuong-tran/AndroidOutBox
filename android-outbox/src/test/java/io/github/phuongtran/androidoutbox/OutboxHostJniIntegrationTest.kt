@file:Suppress("TestFileNamingAndPackage")

package io.github.phuongtran.androidoutbox

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Assume.assumeTrue
import org.junit.Test
import java.io.File
import java.io.FileDescriptor
import java.io.FileInputStream
import java.io.FileOutputStream
import java.lang.reflect.Field
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files

/**
 * Opt-in integration test for the Kotlin/native boundary without Android.
 *
 * Gradle builds a host-loadable shared library from the production C core plus
 * a tiny test JNI bridge. This test then talks to that library through real OS
 * pipes and the production [OutboxNativeControlClient]. The scenarios
 * cover both the current single-consumer shape and the multi-consumer cursor contract,
 * which lets us exercise the Kotlin/native wire protocol without coupling the
 * feedback loop to Android instrumentation.
 */
class OutboxHostJniIntegrationTest {

    @Test(timeout = TEST_TIMEOUT_MS)
    fun `host jni shared library should verify consumer scenarios through native pipes`() {
        val singleConsumerResult = runSingleConsumerScenario()
        val multiConsumerResult = runMultiConsumerScenario()

        // Emit a compact diagnostic summary only after the native path has
        // completed, so output formatting cannot affect the behavior under
        // test and reviewers can still copy a readable result.
        println(
            """
                {
                  "test": "android_outbox_host_jni_test",
                  "result": "passed",
                  "kind": "jni",
                  "scenarios": [
                    {
                      "scenario": "${singleConsumerResult.scenario}",
                      "consumers": "${singleConsumerResult.consumers}",
                      "sources": "${singleConsumerResult.sources}",
                      "records": ${singleConsumerResult.records},
                      "acked_batches": ${singleConsumerResult.ackedBatches}
                    },
                    {
                      "scenario": "${multiConsumerResult.scenario}",
                      "consumers": "${multiConsumerResult.consumers}",
                      "sources": "${multiConsumerResult.sources}",
                      "records": ${multiConsumerResult.records},
                      "acked_batches": ${multiConsumerResult.ackedBatches}
                    }
                  ]
                }
            """.trimIndent(),
        )
    }

    private fun runSingleConsumerScenario(): HostJniScenarioResult {
        return withHostClient { client, spoolDirectory ->
            configureHostLogger(
                client = client,
                spoolDirectory = spoolDirectory,
                defaultProviderId = PRIMARY_PROVIDER_ID,
            )
            assertTrue(
                client.write(
                    level = OutboxRecordLevel.INFO,
                    category = "host.jvm",
                    payload = """{"message":"hello from host jni"}""",
                ),
            )
            assertTrue(client.flush())
            assertEquals(
                OutboxDoorbellEvent.DATA_AVAILABLE,
                client.readNextDoorbell(),
            )

            val batch = client.readNextBatch(
                providerId = PRIMARY_PROVIDER_ID,
                maxRecords = 10,
                maxBytes = 4096,
            )
            assertNotNull(batch)
            requireNotNull(batch)
            assertEquals(1, batch.records.size)
            assertTrue(batch.records.first().contains("host.jvm"))
            assertTrue(batch.records.first().contains("hello from host jni"))
            val acked = client.ack(providerId = PRIMARY_PROVIDER_ID, ackToken = batch.ackToken)
            assertTrue(acked)

            val stats = client.getStats()
            assertTrue(stats.isStarted)
            assertEquals(1L, stats.acceptedCount)
            assertEquals(1L, stats.writtenCount)
            HostJniScenarioResult(
                scenario = SINGLE_SINK_SCENARIO,
                consumers = PRIMARY_PROVIDER_ID,
                sources = "host.jvm",
                records = batch.records.size,
                ackedBatches = 1,
            )
        }
    }

    private fun runMultiConsumerScenario(): HostJniScenarioResult {
        return withHostClient { client, spoolDirectory ->
            configureHostLogger(
                client = client,
                spoolDirectory = spoolDirectory,
                defaultProviderId = PRIMARY_PROVIDER_ID,
            )
            assertTrue(
                client.write(
                    level = OutboxRecordLevel.INFO,
                    category = "host.jvm.first",
                    payload = """{"message":"first"}""",
                ),
            )
            assertTrue(
                client.write(
                    level = OutboxRecordLevel.INFO,
                    category = "host.jvm.second",
                    payload = """{"message":"second"}""",
                ),
            )
            assertTrue(client.flush())
            assertEquals(
                OutboxDoorbellEvent.DATA_AVAILABLE,
                client.readNextDoorbell(),
            )

            val primaryFirstBatch = client.readNextBatch(
                providerId = PRIMARY_PROVIDER_ID,
                maxRecords = 1,
                maxBytes = 4096,
            )
            assertNotNull(primaryFirstBatch)
            requireNotNull(primaryFirstBatch)
            assertEquals(1, primaryFirstBatch.records.size)
            assertTrue(primaryFirstBatch.records.first().contains("host.jvm.first"))
            val primaryFirstAcked = client.ack(
                providerId = PRIMARY_PROVIDER_ID,
                ackToken = primaryFirstBatch.ackToken,
            )
            assertTrue(primaryFirstAcked)

            val secondaryBatch = client.readNextBatch(
                providerId = SECONDARY_PROVIDER_ID,
                maxRecords = 2,
                maxBytes = 4096,
            )
            assertNotNull(secondaryBatch)
            requireNotNull(secondaryBatch)
            assertEquals(2, secondaryBatch.records.size)
            assertTrue(secondaryBatch.records[0].contains("host.jvm.first"))
            assertTrue(secondaryBatch.records[1].contains("host.jvm.second"))
            val secondaryAcked = client.ack(
                providerId = SECONDARY_PROVIDER_ID,
                ackToken = secondaryBatch.ackToken,
            )
            assertTrue(secondaryAcked)

            val primarySecondBatch = client.readNextBatch(
                providerId = PRIMARY_PROVIDER_ID,
                maxRecords = 1,
                maxBytes = 4096,
            )
            assertNotNull(primarySecondBatch)
            requireNotNull(primarySecondBatch)
            assertEquals(1, primarySecondBatch.records.size)
            assertTrue(primarySecondBatch.records.first().contains("host.jvm.second"))
            val primarySecondAcked = client.ack(
                providerId = PRIMARY_PROVIDER_ID,
                ackToken = primarySecondBatch.ackToken,
            )
            assertTrue(primarySecondAcked)
            // The multi-consumer scenario proves provider cursors are independent:
            // one consumer can ack the first record while another still reads
            // the same shared spool from the beginning.
            HostJniScenarioResult(
                scenario = MULTI_SINK_SCENARIO,
                consumers = "$PRIMARY_PROVIDER_ID,$SECONDARY_PROVIDER_ID",
                sources = "host.jvm.first,host.jvm.second",
                records = secondaryBatch.records.size,
                ackedBatches = 3,
            )
        }
    }

    private fun <T> withHostClient(
        block: (OutboxNativeControlClient, File) -> T,
    ): T {
        assumeTrue(HostOutboxNativeBridge.load())
        val spoolDirectory = Files
            .createTempDirectory("observability-host-jni")
            .toFile()
        val transport = HostOutboxPipeTransport.adopt(
            HostOutboxNativeBridge.nativeOpenPipes(),
        )
        val client = OutboxNativeControlClient(transport)

        try {
            assertEquals(
                OutboxDoorbellEvent.HANDSHAKE,
                client.readNextDoorbell(),
            )
            return block(client, spoolDirectory)
        } finally {
            client.stopNativeLogger()
            client.closeNativePipes()
            client.close()
            // Test artifacts must not leak into /tmp; keep cleanup quiet unless
            // deletion fails, in which case the JVM removes it on exit.
            val deleted = runCatching {
                spoolDirectory.deleteRecursively()
            }.getOrDefault(false)
            if (!deleted) {
                spoolDirectory.deleteOnExit()
            }
        }
    }

    private fun configureHostLogger(
        client: OutboxNativeControlClient,
        spoolDirectory: File,
        defaultProviderId: String,
    ) {
        assertTrue(
            client.configure(
                OutboxConfig(
                    spoolDirectoryPath = spoolDirectory.absolutePath,
                    defaultProviderId = defaultProviderId,
                    queueCapacity = 8,
                    maxRecordBytes = 1024,
                    maxSegmentSizeBytes = 4096L,
                    maxArchivedSegments = 1,
                ),
            ),
        )
    }

    private data class HostJniScenarioResult(
        val scenario: String,
        val consumers: String,
        val sources: String,
        val records: Int,
        val ackedBatches: Int,
    )

    private class HostOutboxPipeTransport private constructor(
        private val commandOutputStream: FileOutputStream,
        private val doorbellInputStream: FileInputStream,
        private val recordInputStream: FileInputStream,
    ) : OutboxPipeTransport {
        private val commandOutputChannel: FileChannel = commandOutputStream.channel
        private val doorbellInputChannel: FileChannel = doorbellInputStream.channel
        private val recordInputChannel: FileChannel = recordInputStream.channel
        private val commandHeaderBuffer = OutboxFdFrameCodec.newHeaderBuffer()
        private val doorbellHeaderBuffer = OutboxFdFrameCodec.newHeaderBuffer()
        private val recordHeaderBuffer = OutboxFdFrameCodec.newHeaderBuffer()

        override fun writeCommandEnvelope(envelope: ByteBuffer): Boolean {
            return runCatching {
                synchronized(commandOutputChannel) {
                    OutboxFdFrameCodec.writeFrame(
                        outputChannel = commandOutputChannel,
                        payload = envelope.duplicate(),
                        headerBuffer = commandHeaderBuffer,
                    )
                }
                true
            }.getOrDefault(false)
        }

        override fun readDoorbellFrame(): ByteArray? {
            return runCatching {
                OutboxFdFrameCodec.readFrame(
                    inputChannel = doorbellInputChannel,
                    headerBuffer = doorbellHeaderBuffer,
                )
            }.getOrNull()
        }

        override fun readControlResponseFrame(): ByteArray? {
            return runCatching {
                OutboxFdFrameCodec.readFrame(
                    inputChannel = recordInputChannel,
                    headerBuffer = recordHeaderBuffer,
                )
            }.getOrNull()
        }

        override fun close() {
            runCatching {
                commandOutputStream.close()
            }
            runCatching {
                doorbellInputStream.close()
            }
            runCatching {
                recordInputStream.close()
            }
        }

        companion object {
            /**
             * Wraps raw host pipe fds returned by JNI in JVM streams/channels.
             *
             * This is test-only because the JDK does not expose a public
             * constructor for FileDescriptor(fd). Gradle opens java.io for this
             * opt-in test task so we can exercise the same fd protocol without
             * an emulator.
             */
            fun adopt(fds: IntArray?): HostOutboxPipeTransport {
                requireNotNull(fds) {
                    "Host native logger did not return pipe fds"
                }
                require(fds.size == PIPE_COUNT) {
                    "Expected $PIPE_COUNT pipe fds, received ${fds.size}"
                }
                return HostOutboxPipeTransport(
                    commandOutputStream = FileOutputStream(fileDescriptorFromFd(fds[COMMAND_WRITE_FD_INDEX])),
                    doorbellInputStream = FileInputStream(fileDescriptorFromFd(fds[DOORBELL_READ_FD_INDEX])),
                    recordInputStream = FileInputStream(fileDescriptorFromFd(fds[RECORD_READ_FD_INDEX])),
                )
            }

            private fun fileDescriptorFromFd(fd: Int): FileDescriptor {
                return FileDescriptor().apply {
                    fileDescriptorField.setInt(this, fd)
                }
            }

            private val fileDescriptorField: Field = FileDescriptor::class
                .java
                .getDeclaredField("fd")
                .apply {
                    isAccessible = true
                }

            private const val PIPE_COUNT = 3
            private const val COMMAND_WRITE_FD_INDEX = 0
            private const val DOORBELL_READ_FD_INDEX = 1
            private const val RECORD_READ_FD_INDEX = 2
        }
    }

    private companion object {
        const val TEST_TIMEOUT_MS = 10_000L
        const val SINGLE_SINK_SCENARIO = "single-consumer"
        const val MULTI_SINK_SCENARIO = "multi-consumer"
        const val PRIMARY_PROVIDER_ID = "primary"
        const val SECONDARY_PROVIDER_ID = "secondary"
    }
}
