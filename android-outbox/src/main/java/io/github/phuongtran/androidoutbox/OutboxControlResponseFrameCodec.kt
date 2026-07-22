package io.github.phuongtran.androidoutbox

import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * Native response to one control command.
 *
 * [sequence] and [command] mirror the request so [OutboxNativeControlClient]
 * can reject stale or out-of-order frames instead of accidentally accepting a
 * response intended for another command.
 *
 * @property sequence Control command sequence echoed by native.
 * @property command Control command id echoed by native.
 * @property status Native status code for the command.
 * @property body Command-specific response payload.
 */
internal data class OutboxControlResponseFrame(
    val sequence: Long,
    val command: Int,
    val status: Int,
    val body: ByteArray,
) {
    val isOk: Boolean
        get() = status == STATUS_OK

    companion object {
        const val STATUS_OK = 0
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as OutboxControlResponseFrame

        if (sequence != other.sequence) return false
        if (command != other.command) return false
        if (status != other.status) return false
        if (!body.contentEquals(other.body)) return false
        if (isOk != other.isOk) return false

        return true
    }

    override fun hashCode(): Int {
        var result = sequence.hashCode()
        result = 31 * result + command
        result = 31 * result + status
        result = 31 * result + body.contentHashCode()
        result = 31 * result + isOk.hashCode()
        return result
    }
}

/**
 * Decodes native command responses from the response pipe.
 *
 * The response carries the original sequence and command id so Kotlin can keep
 * the transport byte-oriented without adding JNI methods per operation.
 */
internal object OutboxControlResponseFrameCodec {
    fun decode(frame: ByteArray): OutboxControlResponseFrame? {
        val buffer = ByteBuffer.wrap(frame).order(ByteOrder.LITTLE_ENDIAN)
        if (buffer.remaining() < HEADER_BYTES) {
            return null
        }
        val sequence = buffer.long
        val command = buffer.int
        val status = buffer.int
        val body = ByteArray(buffer.remaining())
        buffer.get(body)
        return OutboxControlResponseFrame(
            sequence = sequence,
            command = command,
            status = status,
            body = body,
        )
    }

    private const val HEADER_BYTES = 16
}
