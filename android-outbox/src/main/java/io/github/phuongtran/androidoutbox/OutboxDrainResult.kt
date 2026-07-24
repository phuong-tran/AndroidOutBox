package io.github.phuongtran.androidoutbox

/**
 * Summary returned by one drain pass.
 *
 * Reading a batch is a peek. [ackedBatches] is the committed progress; if
 * [stoppedByFailure] is true, the last loaded batch was not ACKed and can be
 * retried later while it remains retained.
 */
data class OutboxDrainResult(
    val readBatches: Int,
    val sentRecords: Int,
    val ackedBatches: Int,
    val stoppedByFailure: Boolean,
)
