package io.github.phuongtran.androidoutbox

import kotlinx.coroutines.flow.Flow

/**
 * Coroutine-facing doorbell stream for native observability wakeups.
 *
 * The app layer owns the concrete blocking-read implementation so this core
 * contract does not expose fd, dispatcher, or native bridge details.
 * Implementations backed by a native blocking reader should have one collector
 * and fan out drain triggers above this interface when multiple sinks exist.
 */
interface OutboxDoorbellChannel {
    fun events(): Flow<OutboxDoorbellEvent>
}
