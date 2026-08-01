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
            queueCapacity = 8,
            maxRecordBytes = 4 * 1024 * 1024,
            maxSegmentSizeBytes = 8L * 1024L * 1024L,
        )

        assertEquals(4 * 1024 * 1024, config.maxRecordBytes)
        assertEquals(8L * 1024L * 1024L, config.maxSegmentSizeBytes)
    }

    @Test
    fun `rejects configuration above native queue memory budget`() {
        assertThrows(IllegalArgumentException::class.java) {
            OutboxConfig(
                spoolDirectoryPath = "/tmp/android-outbox",
                queueCapacity = OutboxConfig.MAX_CONFIGURED_QUEUE_CAPACITY,
                maxRecordBytes = OutboxConfig.MAX_CONFIGURED_RECORD_BYTES,
            )
        }
    }

    @Test
    fun `rejects configuration above spool budget`() {
        assertThrows(IllegalArgumentException::class.java) {
            OutboxConfig(
                spoolDirectoryPath = "/tmp/android-outbox",
                maxSegmentSizeBytes = OutboxConfig.MAX_CONFIGURED_SEGMENT_SIZE_BYTES,
                maxArchivedSegments = OutboxConfig.MAX_CONFIGURED_ARCHIVED_SEGMENTS,
            )
        }
    }

    @Test
    fun `spool budget includes a record larger than the segment target`() {
        assertThrows(IllegalArgumentException::class.java) {
            OutboxConfig(
                spoolDirectoryPath = "/tmp/android-outbox",
                queueCapacity = 8,
                maxRecordBytes = OutboxConfig.MAX_CONFIGURED_RECORD_BYTES,
                maxSegmentSizeBytes = 1L,
                maxArchivedSegments = OutboxConfig.MAX_CONFIGURED_ARCHIVED_SEGMENTS,
            )
        }
    }
}
