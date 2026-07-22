package io.github.phuongtran.androidoutbox

object NoOpAndroidOutbox : AndroidOutbox {
    override fun start(config: OutboxConfig): Boolean = false

    override fun write(
        level: OutboxRecordLevel,
        category: String,
        payload: String,
    ): Boolean = false

    override fun readNextDoorbell(): OutboxDoorbellEvent? = null

    override fun flush(): Boolean = false

    override fun stop() = Unit

    override fun getStats(): OutboxStats = OutboxStats()

    override fun readNextBatch(
        providerId: String,
        maxRecords: Int,
        maxBytes: Int,
    ): OutboxBatch? = null

    override fun ack(
        providerId: String,
        ackToken: ByteArray,
    ): Boolean = false
}
