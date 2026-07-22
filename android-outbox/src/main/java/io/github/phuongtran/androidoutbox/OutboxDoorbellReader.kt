package io.github.phuongtran.androidoutbox

/**
 * Blocking reader for native doorbell notifications.
 *
 * The app layer owns when this reader is collected and how each wakeup is
 * translated into durable file reads or transporter work.
 */
interface OutboxDoorbellReader {
    fun readNextDoorbell(): OutboxDoorbellEvent?
}
