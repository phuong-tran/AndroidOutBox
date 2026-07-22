package io.github.phuongtran.androidoutbox

import java.io.Closeable
import java.nio.ByteBuffer

/**
 * Byte-oriented bridge between Kotlin control logic and the native logger.
 *
 * The transport deliberately does not know command ids, response layouts, or
 * record semantics. Production uses Android-owned pipe file descriptors, while
 * host tests can provide JVM file channels around raw fds and exercise the same
 * control client against a host shared library.
 */
internal interface OutboxPipeTransport : Closeable {
    /**
     * Writes one complete command envelope to native.
     *
     * Implementations should serialize writes because the native control thread
     * expects a single ordered command stream.
     */
    fun writeCommandEnvelope(envelope: ByteBuffer): Boolean

    /**
     * Blocks until native sends one doorbell frame, or returns null when the fd
     * is closed or the frame cannot be read safely.
     */
    fun readDoorbellFrame(): ByteArray?

    /**
     * Blocks until native sends one command response frame, or returns null when
     * the fd is closed or the frame cannot be read safely.
     */
    fun readControlResponseFrame(): ByteArray?
}
