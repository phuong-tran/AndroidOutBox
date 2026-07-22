package io.github.phuongtran.androidoutbox

import kotlinx.coroutines.flow.Flow

/**
 * Coroutine-facing doorbell stream for native observability wakeups.
 *
 * The app layer owns the concrete blocking-read implementation so this core
 * contract does not expose fd, dispatcher, or native bridge details.
 */
interface OutboxDoorbellChannel {
    fun events(): Flow<OutboxDoorbellEvent>
}
