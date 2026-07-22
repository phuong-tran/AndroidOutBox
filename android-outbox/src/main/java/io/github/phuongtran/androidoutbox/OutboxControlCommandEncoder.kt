package io.github.phuongtran.androidoutbox

import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * Encodes the outbox control-plane command envelope.
 *
 * This deliberately does not mirror native structs. Kotlin owns policy and
 * writes versioned command bytes, while native parses bytes into its private
 * runtime state. The pipe bridge wraps this envelope in a length-prefixed fd
 * frame so pipe boundaries never leak into command decoding.
 *
 * Envelope layout is little-endian:
 * - magic
 * - version
 * - command
 * - payloadBytes
 * - sequence
 * - payload
 *
 * Command payloads are field lists. Unknown field ids are ignored by native so
 * the protocol can grow without adding JNI methods per operation.
 */
internal object OutboxControlCommandEncoder {
    private const val HEADER_BYTES = 24
    private const val MAGIC = 0x5A4F4253
    private const val VERSION = 1
    const val COMMAND_CONFIGURE = 1
    const val COMMAND_FLUSH = 2
    const val COMMAND_STOP = 3
    const val COMMAND_READ_BATCH = 4
    const val COMMAND_ACK = 5
    const val COMMAND_WRITE = 6
    const val COMMAND_GET_STATS = 7
    const val COMMAND_CLOSE_PIPES = 8
    private const val FIELD_HEADER_BYTES = 12
    private const val FIELD_SPOOL_DIRECTORY_PATH = 1
    private const val FIELD_QUEUE_CAPACITY = 2
    private const val FIELD_MAX_RECORD_BYTES = 3
    private const val FIELD_MAX_SEGMENT_SIZE_BYTES = 4
    private const val FIELD_MAX_ARCHIVED_SEGMENTS = 5
    private const val FIELD_MAX_BATCH_RECORDS = 6
    private const val FIELD_MAX_BATCH_BYTES = 7
    private const val FIELD_ACK_TOKEN = 8
    private const val FIELD_LOG_LEVEL = 9
    private const val FIELD_LOG_CATEGORY = 10
    private const val FIELD_LOG_PAYLOAD = 11
    private const val FIELD_PROVIDER_ID = 12
    private const val VALUE_STRING = 1
    private const val VALUE_UINT32 = 2
    private const val VALUE_UINT64 = 3
    private const val VALUE_BYTES = 4

    fun configure(
        sequence: Long,
        config: OutboxConfig,
    ): ByteBuffer {
        val fields = listOf(
            stringField(
                id = FIELD_SPOOL_DIRECTORY_PATH,
                value = config.spoolDirectoryPath,
            ),
            stringField(
                id = FIELD_PROVIDER_ID,
                value = config.defaultProviderId,
            ),
            uint32Field(
                id = FIELD_QUEUE_CAPACITY,
                value = config.queueCapacity,
            ),
            uint32Field(
                id = FIELD_MAX_RECORD_BYTES,
                value = config.maxRecordBytes,
            ),
            uint64Field(
                id = FIELD_MAX_SEGMENT_SIZE_BYTES,
                value = config.maxSegmentSizeBytes,
            ),
            uint32Field(
                id = FIELD_MAX_ARCHIVED_SEGMENTS,
                value = config.maxArchivedSegments,
            ),
        )
        val payloadBytes = Int.SIZE_BYTES + fields.sumOf(ByteArray::size)
        val payload = ByteBuffer.allocate(payloadBytes).order(ByteOrder.LITTLE_ENDIAN)
        payload.putInt(fields.size)
        fields.forEach(payload::put)
        return frame(
            command = COMMAND_CONFIGURE,
            sequence = sequence,
            payload = payload.array(),
        )
    }

    fun flush(sequence: Long): ByteBuffer {
        return frame(
            command = COMMAND_FLUSH,
            sequence = sequence,
            payload = ByteArray(0),
        )
    }

    fun stop(sequence: Long): ByteBuffer {
        return frame(
            command = COMMAND_STOP,
            sequence = sequence,
            payload = ByteArray(0),
        )
    }

    fun readBatch(
        sequence: Long,
        providerId: String,
        maxRecords: Int,
        maxBytes: Int,
    ): ByteBuffer {
        val fields = listOf(
            stringField(
                id = FIELD_PROVIDER_ID,
                value = providerId,
            ),
            uint32Field(
                id = FIELD_MAX_BATCH_RECORDS,
                value = maxRecords,
            ),
            uint32Field(
                id = FIELD_MAX_BATCH_BYTES,
                value = maxBytes,
            ),
        )
        val payloadBytes = Int.SIZE_BYTES + fields.sumOf(ByteArray::size)
        val payload = ByteBuffer.allocate(payloadBytes).order(ByteOrder.LITTLE_ENDIAN)
        payload.putInt(fields.size)
        fields.forEach(payload::put)
        return frame(
            command = COMMAND_READ_BATCH,
            sequence = sequence,
            payload = payload.array(),
        )
    }

    fun ack(
        sequence: Long,
        providerId: String,
        ackToken: ByteArray,
    ): ByteBuffer {
        val fields = listOf(
            stringField(
                id = FIELD_PROVIDER_ID,
                value = providerId,
            ),
            bytesField(
                id = FIELD_ACK_TOKEN,
                value = ackToken,
            ),
        )
        val payloadBytes = Int.SIZE_BYTES + fields.sumOf(ByteArray::size)
        val payload = ByteBuffer.allocate(payloadBytes).order(ByteOrder.LITTLE_ENDIAN)
        payload.putInt(fields.size)
        fields.forEach(payload::put)
        return frame(
            command = COMMAND_ACK,
            sequence = sequence,
            payload = payload.array(),
        )
    }

    fun write(
        sequence: Long,
        level: OutboxRecordLevel,
        category: String,
        payload: String,
    ): ByteBuffer {
        val categoryBytes = category.toByteArray(Charsets.UTF_8)
        val payloadBytes = payload.toByteArray(Charsets.UTF_8)
        val commandPayload = ByteBuffer
            .allocate(
                Int.SIZE_BYTES +
                    FIELD_HEADER_BYTES + Int.SIZE_BYTES +
                    FIELD_HEADER_BYTES + categoryBytes.size +
                    FIELD_HEADER_BYTES + payloadBytes.size,
            )
            .order(ByteOrder.LITTLE_ENDIAN)
        commandPayload.putInt(LOG_FIELD_COUNT)
        putFieldHeader(
            buffer = commandPayload,
            id = FIELD_LOG_LEVEL,
            valueType = VALUE_UINT32,
            valueSize = Int.SIZE_BYTES,
        )
        commandPayload.putInt(level.nativeValue)
        putFieldHeader(
            buffer = commandPayload,
            id = FIELD_LOG_CATEGORY,
            valueType = VALUE_STRING,
            valueSize = categoryBytes.size,
        )
        commandPayload.put(categoryBytes)
        putFieldHeader(
            buffer = commandPayload,
            id = FIELD_LOG_PAYLOAD,
            valueType = VALUE_STRING,
            valueSize = payloadBytes.size,
        )
        commandPayload.put(payloadBytes)
        return frame(
            command = COMMAND_WRITE,
            sequence = sequence,
            payload = commandPayload.array(),
        )
    }

    fun getStats(sequence: Long): ByteBuffer {
        return frame(
            command = COMMAND_GET_STATS,
            sequence = sequence,
            payload = ByteArray(0),
        )
    }

    fun closePipes(sequence: Long): ByteBuffer {
        return frame(
            command = COMMAND_CLOSE_PIPES,
            sequence = sequence,
            payload = ByteArray(0),
        )
    }

    private fun frame(
        command: Int,
        sequence: Long,
        payload: ByteArray,
    ): ByteBuffer {
        val buffer = ByteBuffer.allocate(HEADER_BYTES + payload.size).order(ByteOrder.LITTLE_ENDIAN)
        buffer.putInt(MAGIC)
        buffer.putInt(VERSION)
        buffer.putInt(command)
        buffer.putInt(payload.size)
        buffer.putLong(sequence)
        buffer.put(payload)
        buffer.flip()
        return buffer
    }

    private fun stringField(
        id: Int,
        value: String,
    ): ByteArray {
        return field(
            id = id,
            valueType = VALUE_STRING,
            value = value.toByteArray(Charsets.UTF_8),
        )
    }

    private fun uint32Field(
        id: Int,
        value: Int,
    ): ByteArray {
        val bytes = ByteBuffer
            .allocate(Int.SIZE_BYTES)
            .order(ByteOrder.LITTLE_ENDIAN)
            .putInt(value)
            .array()
        return field(
            id = id,
            valueType = VALUE_UINT32,
            value = bytes,
        )
    }

    private fun uint64Field(
        id: Int,
        value: Long,
    ): ByteArray {
        val bytes = ByteBuffer
            .allocate(Long.SIZE_BYTES)
            .order(ByteOrder.LITTLE_ENDIAN)
            .putLong(value)
            .array()
        return field(
            id = id,
            valueType = VALUE_UINT64,
            value = bytes,
        )
    }

    private fun bytesField(
        id: Int,
        value: ByteArray,
    ): ByteArray {
        return field(
            id = id,
            valueType = VALUE_BYTES,
            value = value,
        )
    }

    private fun field(
        id: Int,
        valueType: Int,
        value: ByteArray,
    ): ByteArray {
        return ByteBuffer
            .allocate(FIELD_HEADER_BYTES + value.size)
            .order(ByteOrder.LITTLE_ENDIAN)
            .putInt(id)
            .putInt(valueType)
            .putInt(value.size)
            .put(value)
            .array()
    }

    private fun putFieldHeader(
        buffer: ByteBuffer,
        id: Int,
        valueType: Int,
        valueSize: Int,
    ) {
        buffer.putInt(id)
        buffer.putInt(valueType)
        buffer.putInt(valueSize)
    }

    private const val LOG_FIELD_COUNT = 3
}
