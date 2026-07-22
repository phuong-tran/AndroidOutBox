package io.github.phuongtran.androidoutbox

import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.nio.ByteBuffer
import java.nio.ByteOrder

class OutboxControlResponseFrameCodecTest {
    @Test
    fun `decodes command response envelope`() {
        val body = byteArrayOf(1, 2, 3, 4)

        val response = OutboxControlResponseFrameCodec.decode(
            responseFrame(
                sequence = 42L,
                command = OutboxControlCommandEncoder.COMMAND_GET_STATS,
                status = OutboxControlResponseFrame.STATUS_OK,
                body = body,
            ),
        )

        requireNotNull(response)
        assertEquals(42L, response.sequence)
        assertEquals(OutboxControlCommandEncoder.COMMAND_GET_STATS, response.command)
        assertEquals(OutboxControlResponseFrame.STATUS_OK, response.status)
        assertArrayEquals(body, response.body)
        assertTrue(response.isOk)
    }

    @Test
    fun `returns null for truncated response envelope`() {
        assertNull(OutboxControlResponseFrameCodec.decode(ByteArray(4)))
    }

    private fun responseFrame(
        sequence: Long,
        command: Int,
        status: Int,
        body: ByteArray,
    ): ByteArray {
        return ByteBuffer
            .allocate(HEADER_BYTES + body.size)
            .order(ByteOrder.LITTLE_ENDIAN)
            .putLong(sequence)
            .putInt(command)
            .putInt(status)
            .put(body)
            .array()
    }

    private companion object {
        const val HEADER_BYTES = 16
    }
}
