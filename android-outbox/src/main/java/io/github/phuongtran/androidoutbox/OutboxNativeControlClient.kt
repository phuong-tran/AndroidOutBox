package io.github.phuongtran.androidoutbox

import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * Serializes Kotlin-owned control commands over the native pipe transport.
 *
 * Public outbox APIs should not know about sequence numbers, response matching,
 * or native response body layouts. Keeping that protocol here makes the global
 * outbox object a lifecycle facade instead of a control-plane implementation.
 *
 * All commands that expect a response share [commandLock]. Native processes the
 * command pipe sequentially, and Kotlin reads one response pipe, so allowing
 * concurrent command/response pairs would make sequence matching ambiguous.
 */
internal class OutboxNativeControlClient(
    private val transport: OutboxPipeTransport,
) : Closeable {
    private val commandLock = Any()
    private var controlSequence = 1L

    @Volatile
    private var closed = false

    /**
     * Starts the native writer with bounded queue and spool limits.
     */
    fun configure(config: OutboxConfig): Boolean {
        return sendCommandAndReadOk(
            command = OutboxControlCommandEncoder.COMMAND_CONFIGURE,
        ) { sequence ->
            OutboxControlCommandEncoder.configure(sequence, config)
        }
    }

    /**
     * Sends one record to native without waiting for an enqueue response.
     *
     * A true result means Kotlin handed a complete frame to the native command
     * pipe. Native still owns bounded enqueue, persistence, and drop accounting.
     * This keeps writing off the command/response round trip.
     */
    fun write(
        level: OutboxRecordLevel,
        category: String,
        payload: String,
    ): Boolean {
        if (category.isBlank() || payload.isBlank()) {
            return false
        }
        return synchronized(commandLock) {
            if (closed) {
                return@synchronized false
            }
            val sequence = nextControlSequence()
            OutboxControlCommandEncoder.write(
                sequence = sequence,
                level = level,
                category = category,
                payload = payload,
            ).let(transport::writeCommandEnvelope)
        }
    }

    /**
     * Waits until records accepted before this command have reached the spool.
     */
    fun flush(): Boolean {
        return sendCommandAndReadOk(
            command = OutboxControlCommandEncoder.COMMAND_FLUSH,
        ) { sequence ->
            OutboxControlCommandEncoder.flush(sequence)
        }
    }

    /**
     * Stops the native writer thread but keeps the control pipes available long
     * enough for Kotlin to close them in an ordered shutdown.
     */
    fun stopNativeLogger(): Boolean {
        return sendCommandAndReadOk(
            command = OutboxControlCommandEncoder.COMMAND_STOP,
        ) { sequence ->
            OutboxControlCommandEncoder.stop(sequence)
        }
    }

    /**
     * Reads native counters for diagnostics and tests. Any malformed response
     * falls back to an empty stats object rather than throwing from outbox code.
     */
    fun getStats(): OutboxStats {
        return synchronized(commandLock) {
            if (closed) {
                return@synchronized OutboxStats()
            }
            val sequence = nextControlSequence()
            if (!transport.writeCommandEnvelope(OutboxControlCommandEncoder.getStats(sequence))) {
                return@synchronized OutboxStats()
            }
            val response = readExpectedResponse(
                sequence = sequence,
                command = OutboxControlCommandEncoder.COMMAND_GET_STATS,
            ) ?: return@synchronized OutboxStats()
            if (!response.isOk) {
                return@synchronized OutboxStats()
            }
            response.body.toOutboxStats()
        }
    }

    /**
     * Blocks for the next native wakeup. Doorbells contain only event ids; log
     * records are always pulled separately through [readNextBatch].
     */
    fun readNextDoorbell(): OutboxDoorbellEvent? {
        if (closed) {
            return null
        }
        val payload = transport.readDoorbellFrame() ?: return null
        if (payload.size != DOORBELL_PAYLOAD_BYTES) {
            return OutboxDoorbellEvent.UNKNOWN
        }
        val nativeValue = ByteBuffer
            .wrap(payload)
            .order(ByteOrder.LITTLE_ENDIAN)
            .int
        return OutboxDoorbellEvent.fromNativeValue(nativeValue)
    }

    /**
     * Peeks durable records from the native spool without advancing delivery
     * state. Callers must pass [OutboxBatch.ackToken] to [ack] only
     * after upload succeeds.
     */
    fun readNextBatch(
        providerId: String,
        maxRecords: Int,
        maxBytes: Int,
    ): OutboxBatch? {
        if (
            providerId.isBlank() ||
            maxRecords <= 0 ||
            maxBytes <= 0
        ) {
            return null
        }
        return synchronized(commandLock) {
            if (closed) {
                return@synchronized null
            }
            val sequence = nextControlSequence()
            if (!transport.writeCommandEnvelope(
                    OutboxControlCommandEncoder.readBatch(
                        sequence = sequence,
                        providerId = providerId,
                        maxRecords = maxRecords,
                        maxBytes = maxBytes,
                    ),
                )
            ) {
                return@synchronized null
            }
            val response = readExpectedResponse(
                sequence = sequence,
                command = OutboxControlCommandEncoder.COMMAND_READ_BATCH,
            ) ?: return@synchronized null
            if (!response.isOk) {
                return@synchronized null
            }
            OutboxBatchFrameCodec.decode(response.body)
        }
    }

    /**
     * Commits delivery progress after a remote consumer accepts every record in a
     * batch. The token is native-owned and intentionally opaque to Kotlin.
     */
    fun ack(
        providerId: String,
        ackToken: ByteArray,
    ): Boolean {
        if (providerId.isBlank() || ackToken.isEmpty()) {
            return false
        }
        return sendCommandAndReadOk(
            command = OutboxControlCommandEncoder.COMMAND_ACK,
        ) { sequence ->
            OutboxControlCommandEncoder.ack(
                sequence = sequence,
                providerId = providerId,
                ackToken = ackToken,
            )
        }
    }

    /**
     * Asks native to close its pipe ends and stop the control thread.
     */
    fun closeNativePipes(): Boolean {
        return sendCommandAndReadOk(
            command = OutboxControlCommandEncoder.COMMAND_CLOSE_PIPES,
        ) { sequence ->
            OutboxControlCommandEncoder.closePipes(sequence)
        }
    }

    override fun close() {
        synchronized(commandLock) {
            if (closed) {
                return
            }
            closed = true
            transport.close()
        }
    }

    private fun sendCommandAndReadOk(
        command: Int,
        frameBuilder: (sequence: Long) -> ByteBuffer,
    ): Boolean {
        return synchronized(commandLock) {
            if (closed) {
                return@synchronized false
            }
            val sequence = nextControlSequence()
            if (!transport.writeCommandEnvelope(frameBuilder(sequence))) {
                return@synchronized false
            }
            readExpectedResponse(
                sequence = sequence,
                command = command,
            )?.isOk == true
        }
    }

    private fun readExpectedResponse(
        sequence: Long,
        command: Int,
    ): OutboxControlResponseFrame? {
        val response = OutboxControlResponseFrameCodec.decode(
            transport.readControlResponseFrame() ?: return null,
        ) ?: return null
        if (response.sequence != sequence || response.command != command) {
            return null
        }
        return response
    }

    private fun nextControlSequence(): Long {
        val sequence = controlSequence
        controlSequence += 1L
        return sequence
    }

    private fun ByteArray.toOutboxStats(): OutboxStats {
        if (size < STATS_BODY_BYTES) {
            return OutboxStats()
        }
        val buffer = ByteBuffer.wrap(this).order(ByteOrder.LITTLE_ENDIAN)
        return OutboxStats(
            isStarted = buffer.long != 0L,
            queueCapacity = buffer.long.toInt(),
            queueDepth = buffer.long.toInt(),
            queueHighWatermark = buffer.long.toInt(),
            nextSequence = buffer.long,
            acceptedCount = buffer.long,
            writtenCount = buffer.long,
            droppedQueueFullCount = buffer.long,
            droppedInvalidCount = buffer.long,
            droppedRecordTooLargeCount = buffer.long,
            writeFailureCount = buffer.long,
            currentFileSizeBytes = buffer.long,
            rollCount = buffer.long,
        )
    }

    private companion object {
        const val DOORBELL_PAYLOAD_BYTES = 4
        const val STATS_SIZE = 13
        const val STATS_BODY_BYTES = STATS_SIZE * Long.SIZE_BYTES
    }
}
