package io.github.phuongtran.androidoutbox

/**
 * Opaque native-owned delivery batch.
 *
 * Kotlin must not parse [ackToken]. It should only pass the token back after a
 * remote transporter confirms that every record in the batch was accepted.
 *
 * @property ackToken Native cursor token to ACK after successful transport or
 * intentional skip policy.
 * @property records Raw spool lines returned by native for one provider cursor.
 */
data class OutboxBatch(
    val ackToken: ByteArray,
    val records: List<String>,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as OutboxBatch

        if (!ackToken.contentEquals(other.ackToken)) return false
        if (records != other.records) return false

        return true
    }

    override fun hashCode(): Int {
        var result = ackToken.contentHashCode()
        result = 31 * result + records.hashCode()
        return result
    }
}
