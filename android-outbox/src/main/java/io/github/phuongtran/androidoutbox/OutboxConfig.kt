package io.github.phuongtran.androidoutbox

/**
 * Runtime limits for the native file writer.
 *
 * Defaults are intentionally small because this logger is expected to absorb
 * bursts and preserve recent failure context, not become long-term storage.
 *
 * @property spoolDirectoryPath App-private directory where native segment and
 * cursor files are stored. Prefer cache/internal storage. The logger owns files
 * inside this directory and may delete old segment files to enforce
 * [maxArchivedSegments].
 * @property defaultProviderId Opaque delivery cursor id used by the default
 * drain path. Native does not know what backend or consumer this id represents.
 * It only stores a cursor for this provider so consumers can keep separate
 * read/ack progress over the same shared spool.
 * @property queueCapacity Number of records that can wait in memory before
 * producers start dropping. This is a bounded native MPSC queue, not durable
 * storage. A higher value absorbs bursts better but increases native memory
 * reserved at startup.
 * @property maxRecordBytes Maximum payload bytes accepted for one record.
 * Records at or above this size are rejected before enqueueing. Callers should
 * keep payloads compact, sanitized, and single-line.
 * @property maxSegmentSizeBytes Maximum size of one spool segment before the
 * writer rolls to a new file. The disk budget is approximately
 * `maxSegmentSizeBytes * (maxArchivedSegments + 1)` because the active segment
 * is kept in addition to archived segments.
 * @property maxArchivedSegments Number of rolled segment files to retain beside
 * the active segment. When the segment count exceeds this value plus the active
 * segment, oldest segments are deleted even if they have not been uploaded yet.
 * This keeps the logger best-effort and bounded rather than an unbounded audit
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
        require(maxRecordBytes > 0) {
            "maxRecordBytes must be greater than 0"
        }
        require(maxSegmentSizeBytes > 0L) {
            "maxSegmentSizeBytes must be greater than 0"
        }
        require(maxArchivedSegments >= 0) {
            "maxArchivedSegments must not be negative"
        }
    }

    companion object {
        const val DEFAULT_QUEUE_CAPACITY = 256
        const val DEFAULT_PROVIDER_ID = "default"
        const val DEFAULT_MAX_RECORD_BYTES = 4 * 1024
        const val DEFAULT_MAX_SEGMENT_SIZE_BYTES = 512L * 1024L
        const val DEFAULT_MAX_ARCHIVED_SEGMENTS = 3
        const val MAX_PIPE_FRAME_BYTES = Int.MAX_VALUE
        private val PROVIDER_ID_REGEX = Regex("[A-Za-z0-9._-]{1,63}")
    }
}
