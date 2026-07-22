package io.github.phuongtran.androidoutbox

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Test

class NoOpAndroidOutboxTest {
    @Test
    fun `returns fail-open defaults`() {
        val config = OutboxConfig(spoolDirectoryPath = "/tmp/android-outbox")

        assertFalse(NoOpAndroidOutbox.start(config))
        assertFalse(
            NoOpAndroidOutbox.write(
                level = OutboxRecordLevel.ERROR,
                category = "network.http",
                payload = """{"status_code":500}""",
            ),
        )
        assertFalse(NoOpAndroidOutbox.flush())
        assertNull(NoOpAndroidOutbox.readNextBatch())
        assertFalse(
            NoOpAndroidOutbox.ack(
                ackToken = "00000000000000000001:00000000000000000010".toByteArray(),
            ),
        )
        assertEquals(OutboxStats(), NoOpAndroidOutbox.getStats())
    }
}
