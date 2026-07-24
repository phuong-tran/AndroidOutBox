@file:Suppress("TestFileNamingAndPackage")

package io.github.phuongtran.androidoutbox

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Test

/**
 * Opt-in integration test for the Kotlin/native boundary without Android.
 *
 * Gradle builds a host-loadable shared library from the production C core plus
 * a tiny test JNI bridge. This test then talks to that library through real OS
 * pipes and the production [OutboxNativeControlClient]. The scenarios
 * cover both the current single-provider shape and the multi-provider cursor contract,
 * which lets us exercise the Kotlin/native wire protocol without coupling the
 * feedback loop to Android instrumentation.
 */
class OutboxHostJniIntegrationTest {

    @Test(timeout = TEST_TIMEOUT_MS)
    fun `host jni shared library should verify provider scenarios through native pipes`() {
        val singleProviderResult = runSingleProviderScenario()
        val multiProviderResult = runMultiProviderScenario()

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
                      "scenario": "${singleProviderResult.scenario}",
                      "providers": "${singleProviderResult.providers}",
                      "sources": "${singleProviderResult.sources}",
                      "records": ${singleProviderResult.records},
                      "acked_batches": ${singleProviderResult.ackedBatches}
                    },
                    {
                      "scenario": "${multiProviderResult.scenario}",
                      "providers": "${multiProviderResult.providers}",
                      "sources": "${multiProviderResult.sources}",
                      "records": ${multiProviderResult.records},
                      "acked_batches": ${multiProviderResult.ackedBatches}
                    }
                  ]
                }
            """.trimIndent(),
        )
    }

    private fun runSingleProviderScenario(): HostJniScenarioResult {
        return withHostClient { client, spoolDirectory ->
            configureHostOutbox(
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
                scenario = SINGLE_PROVIDER_SCENARIO,
                providers = PRIMARY_PROVIDER_ID,
                sources = "host.jvm",
                records = batch.records.size,
                ackedBatches = 1,
            )
        }
    }

    private fun runMultiProviderScenario(): HostJniScenarioResult {
        return withHostClient { client, spoolDirectory ->
            configureHostOutbox(
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
            // The multi-provider scenario proves provider cursors are independent:
            // primary can ack the first record while secondary still reads
            // the same shared spool from the beginning.
            HostJniScenarioResult(
                scenario = MULTI_PROVIDER_SCENARIO,
                providers = "$PRIMARY_PROVIDER_ID,$SECONDARY_PROVIDER_ID",
                sources = "host.jvm.first,host.jvm.second",
                records = secondaryBatch.records.size,
                ackedBatches = 3,
            )
        }
    }

    private data class HostJniScenarioResult(
        val scenario: String,
        val providers: String,
        val sources: String,
        val records: Int,
        val ackedBatches: Int,
    )

    private companion object {
        const val TEST_TIMEOUT_MS = 10_000L
        const val SINGLE_PROVIDER_SCENARIO = "single-provider"
        const val MULTI_PROVIDER_SCENARIO = "multi-provider"
        const val PRIMARY_PROVIDER_ID = "primary"
        const val SECONDARY_PROVIDER_ID = "secondary"
    }
}
