package io.github.phuongtran.androidoutbox

/**
 * Runtime limits for the native file writer.
 *
 * Defaults are intentionally small because this outbox is expected to absorb
 * bursts and preserve recent failure context, not become long-term storage.
 *
 * @property spoolDirectoryPath App-private directory where native segment and
 * cursor files are stored. Prefer `noBackupFilesDir` or `filesDir` when pending
 * records should survive normal cache cleanup. Use `cacheDir` only when records
 * may be discarded by the operating system. The outbox owns files inside this
 * directory and may delete old segment files to enforce [maxArchivedSegments].
 * @property defaultProviderId Opaque delivery cursor id used by the default
 * drain path. Native does not know what backend or consumer this id represents.
 * It only stores a cursor for this provider so consumers can keep separate
 * read/ack progress over the same shared spool.
 * @property queueCapacity Number of records that can wait in memory before
 * producers start dropping. This is a bounded native MPSC queue, not durable
 * storage. A higher value absorbs bursts better but increases native memory
 * reserved at startup. The configured capacity and record size must also fit
 * within [MAX_IN_MEMORY_QUEUE_BYTES].
 * @property maxRecordBytes Maximum UTF-8 payload bytes accepted for one record.
 * Records at or above this size are rejected before Kotlin writes a command
 * frame to native. Callers should keep payloads compact, sanitized, and
 * single-line.
 * @property maxSegmentSizeBytes Maximum size of one spool segment before the
 * writer rolls to a new file. The disk budget is approximately
 * `maxSegmentSizeBytes * (maxArchivedSegments + 1)` because the active segment
 * is kept in addition to archived segments. That combined budget must not
 * exceed [MAX_CONFIGURED_SPOOL_BYTES].
 * @property maxArchivedSegments Number of rolled segment files to retain beside
 * the active segment. When the segment count exceeds this value plus the active
 * segment, oldest segments are deleted even if they have not been uploaded yet.
 * This keeps the outbox best-effort and bounded rather than an unbounded audit
 * log.
 */
data class OutboxConfig(
    val spoolDirectoryPath: String,
    val defaultProviderId: String = DEFAULT_PROVIDER_ID,
    val queueCapacity: Int = DEFAULT_QUEUE_CAPACITY,
    val maxRecordBytes: Int = DEFAULT_MAX_RECORD_BYTES,
    val maxSegmentSizeBytes: Long = DEFAULT_MAX_SEGMENT_SIZE_BYTES,
    val maxArchivedSegments: Int = DEFAULT_MAX_ARCHIVED_SEGMENTS,
) {
    init {
        require(spoolDirectoryPath.isNotBlank()) {
            "spoolDirectoryPath must not be blank"
        }
        require(defaultProviderId.matches(PROVIDER_ID_REGEX)) {
            "defaultProviderId must contain only letters, digits, '.', '_' or '-'"
        }
        require(queueCapacity > 0) {
            "queueCapacity must be greater than 0"
        }
        require(queueCapacity <= MAX_CONFIGURED_QUEUE_CAPACITY) {
            "queueCapacity must not exceed $MAX_CONFIGURED_QUEUE_CAPACITY"
        }
        require(maxRecordBytes > 0) {
            "maxRecordBytes must be greater than 0"
        }
        require(maxRecordBytes <= MAX_CONFIGURED_RECORD_BYTES) {
            "maxRecordBytes must not exceed $MAX_CONFIGURED_RECORD_BYTES"
        }
        require(
            queueCapacity.toLong() *
                (maxRecordBytes.toLong() + MAX_QUEUE_RECORD_OVERHEAD_BYTES) <=
                MAX_IN_MEMORY_QUEUE_BYTES,
        ) {
            "queueCapacity and maxRecordBytes exceed the native queue memory budget"
        }
        require(maxSegmentSizeBytes > 0L) {
            "maxSegmentSizeBytes must be greater than 0"
        }
        require(maxSegmentSizeBytes <= MAX_CONFIGURED_SEGMENT_SIZE_BYTES) {
            "maxSegmentSizeBytes must not exceed $MAX_CONFIGURED_SEGMENT_SIZE_BYTES"
        }
        require(maxArchivedSegments >= 0) {
            "maxArchivedSegments must not be negative"
        }
        require(maxArchivedSegments <= MAX_CONFIGURED_ARCHIVED_SEGMENTS) {
            "maxArchivedSegments must not exceed $MAX_CONFIGURED_ARCHIVED_SEGMENTS"
        }
        val maxSegmentFootprintBytes = maxOf(
            maxSegmentSizeBytes,
            maxRecordBytes.toLong() + MAX_SPOOL_RECORD_OVERHEAD_BYTES,
        )
        require(
            maxSegmentFootprintBytes <=
                MAX_CONFIGURED_SPOOL_BYTES / (maxArchivedSegments.toLong() + 1L),
        ) {
            "record size, segment size and retention exceed the configured spool budget"
        }
    }

    companion object {
        const val DEFAULT_QUEUE_CAPACITY = 256
        const val DEFAULT_PROVIDER_ID = "default"
        const val DEFAULT_MAX_RECORD_BYTES = 4 * 1024
        const val DEFAULT_MAX_SEGMENT_SIZE_BYTES = 512L * 1024L
        const val DEFAULT_MAX_ARCHIVED_SEGMENTS = 3
        const val MAX_PIPE_FRAME_BYTES = 32 * 1024 * 1024
        const val MAX_CATEGORY_BYTES = 95
        const val MAX_PROVIDER_CURSORS = 8
        const val MAX_CONFIGURED_QUEUE_CAPACITY = 65_536
        const val MAX_CONFIGURED_RECORD_BYTES = 4 * 1024 * 1024
        const val MAX_IN_MEMORY_QUEUE_BYTES = 128L * 1024L * 1024L
        const val MAX_CONFIGURED_SEGMENT_SIZE_BYTES = 128L * 1024L * 1024L
        const val MAX_CONFIGURED_ARCHIVED_SEGMENTS = 255
        const val MAX_CONFIGURED_SPOOL_BYTES = 1024L * 1024L * 1024L
        // Includes slot/category storage plus worst-case 128-byte payload alignment.
        private const val MAX_QUEUE_RECORD_OVERHEAD_BYTES = 384L
        private const val MAX_SPOOL_RECORD_OVERHEAD_BYTES = 96L + 192L
        private val PROVIDER_ID_REGEX = Regex("[A-Za-z0-9._-]{1,63}")
    }
}
