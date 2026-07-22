package io.github.phuongtran.androidoutbox

import org.junit.Assert.assertEquals
import org.junit.Test

class OutboxDoorbellEventTest {
    @Test
    fun `maps native doorbell values to stable events`() {
        assertEquals(
            OutboxDoorbellEvent.HANDSHAKE,
            OutboxDoorbellEvent.fromNativeValue(0),
        )
        assertEquals(
            OutboxDoorbellEvent.DATA_AVAILABLE,
            OutboxDoorbellEvent.fromNativeValue(1),
        )
        assertEquals(
            OutboxDoorbellEvent.DROPPED_RECORD,
            OutboxDoorbellEvent.fromNativeValue(2),
        )
        assertEquals(
            OutboxDoorbellEvent.UNKNOWN,
            OutboxDoorbellEvent.fromNativeValue(99),
        )
    }
}
