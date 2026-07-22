package io.github.phuongtran.androidoutbox

import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Test

class OutboxConfigTest {
    @Test
    fun `throws when spool directory path is blank`() {
        assertThrows(IllegalArgumentException::class.java) {
            OutboxConfig(spoolDirectoryPath = " ")
        }
    }

    @Test
    fun `throws when queue capacity is invalid`() {
        assertThrows(IllegalArgumentException::class.java) {
            OutboxConfig(
                spoolDirectoryPath = "/tmp/android-outbox",
                queueCapacity = 0,
            )
        }
    }

    @Test
    fun `throws when default provider id is invalid`() {
        assertThrows(IllegalArgumentException::class.java) {
            OutboxConfig(
                spoolDirectoryPath = "/tmp/android-outbox",
                defaultProviderId = "../primary",
            )
        }
    }

    @Test
    fun `throws when max archived segments is invalid`() {
        assertThrows(IllegalArgumentException::class.java) {
            OutboxConfig(
                spoolDirectoryPath = "/tmp/android-outbox",
                maxArchivedSegments = -1,
            )
        }
    }

    @Test
    fun `allows megabyte scale record configuration`() {
        val config = OutboxConfig(
            spoolDirectoryPath = "/tmp/android-outbox",
            maxRecordBytes = 4 * 1024 * 1024,
            maxSegmentSizeBytes = 8L * 1024L * 1024L,
        )

        assertEquals(4 * 1024 * 1024, config.maxRecordBytes)
        assertEquals(8L * 1024L * 1024L, config.maxSegmentSizeBytes)
    }
}
