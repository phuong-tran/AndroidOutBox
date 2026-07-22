package io.github.phuongtran.androidoutbox

import java.io.EOFException
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.ReadableByteChannel
import java.nio.channels.WritableByteChannel

/**
 * Length-prefixed fd channel codec shared by native pipe readers and future record transport.
 *
 * Pipe reads are byte streams, not message boundaries. The 4-byte big-endian frame length keeps
 * Kotlin from treating partial or coalesced pipe reads as complete protocol messages.
 */
internal object OutboxFdFrameCodec {
    /**
     * Creates a reusable 4-byte frame header buffer.
     *
     * Callers that own a long-lived pipe should keep one header buffer per
     * channel to avoid allocating on every command or doorbell read.
     */
    fun newHeaderBuffer(): ByteBuffer {
        return ByteBuffer
            .allocate(FRAME_HEADER_BYTES)
            .order(ByteOrder.BIG_ENDIAN)
    }

    /**
     * Reads one length-prefixed frame.
     *
     * Returns null only when the channel is already closed before any header
     * byte is read. A close in the middle of a header/payload is treated as a
     * broken frame and throws [EOFException].
     */
    fun readFrame(
        inputChannel: ReadableByteChannel,
        headerBuffer: ByteBuffer = newHeaderBuffer(),
    ): ByteArray? {
        headerBuffer.clear()
        headerBuffer.limit(FRAME_HEADER_BYTES)
        if (!inputChannel.readFullyOrEof(headerBuffer)) {
            return null
        }

        headerBuffer.flip()
        val size = headerBuffer.int
        require(size in 0..MAX_FRAME_BYTES) {
            "Invalid observability frame size: $size"
        }

        return ByteArray(size).also { payload ->
            if (!inputChannel.readFullyOrEof(ByteBuffer.wrap(payload))) {
                throw EOFException("EOF while reading observability frame payload")
            }
        }
    }

    /**
     * Writes one length-prefixed frame from a byte array payload.
     */
    fun writeFrame(
        outputChannel: WritableByteChannel,
        payload: ByteArray,
        headerBuffer: ByteBuffer = newHeaderBuffer(),
    ) {
        writeFrame(
            outputChannel = outputChannel,
            payload = ByteBuffer.wrap(payload),
            headerBuffer = headerBuffer,
        )
    }

    /**
     * Writes one length-prefixed frame from the payload's current remaining
     * bytes. The caller's buffer position is advanced, so pass a duplicate when
     * the original position must be preserved.
     */
    fun writeFrame(
        outputChannel: WritableByteChannel,
        payload: ByteBuffer,
        headerBuffer: ByteBuffer = newHeaderBuffer(),
    ) {
        require(payload.remaining() <= MAX_FRAME_BYTES) {
            "Outbox frame is too large: ${payload.remaining()}"
        }
        headerBuffer.clear()
        headerBuffer.limit(FRAME_HEADER_BYTES)
        headerBuffer.putInt(payload.remaining())
        headerBuffer.flip()
        outputChannel.writeFully(headerBuffer)
        outputChannel.writeFully(payload)
    }

    /**
     * Pipe channels are expected to block until progress is possible. A 0-byte
     * result on a requested read would otherwise spin forever, so fail loudly.
     */
    private fun ReadableByteChannel.readFullyOrEof(buffer: ByteBuffer): Boolean {
        var hasReadBytes = false
        while (buffer.hasRemaining()) {
            val count = read(buffer)
            if (count < 0) {
                if (!hasReadBytes) {
                    return false
                }
                throw EOFException("Unexpected EOF while reading observability frame")
            }
            if (count == 0) {
                throw EOFException("Read returned 0 bytes while reading observability frame")
            }
            hasReadBytes = true
        }
        return true
    }

    /**
     * Writes can be partial even for one logical frame, so keep writing until
     * both header and payload are fully consumed.
     */
    private fun WritableByteChannel.writeFully(buffer: ByteBuffer) {
        while (buffer.hasRemaining()) {
            val count = write(buffer)
            if (count == 0) {
                throw EOFException("Write returned 0 bytes while writing observability frame")
            }
        }
    }

    private const val FRAME_HEADER_BYTES = 4
    private const val MAX_FRAME_BYTES = OutboxConfig.MAX_PIPE_FRAME_BYTES
}
