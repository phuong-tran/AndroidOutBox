package com.android.pt.outbox

import io.github.phuongtran.androidoutbox.OutboxBatch
import io.github.phuongtran.androidoutbox.OutboxDoorbellEvent
import io.github.phuongtran.androidoutbox.OutboxRecordLevel
import io.github.phuongtran.androidoutbox.OutboxStats

data class OutboxLabUiState(
    val stats: OutboxStats = OutboxStats(),
    val selectedCategory: LabCategory = LAB_CATEGORIES.first(),
    val selectedWriter: LabWriter = LAB_WRITERS.first(),
    val currentBatch: OutboxBatch? = null,
    val lastDoorbellEvent: OutboxDoorbellEvent? = null,
    val doorbellCount: Long = 0L,
    val busyAction: String? = null,
    val consoleLines: List<String> = emptyList(),
) {
    val payloadPreview: String
        get() = selectedWriter.payload(
            category = selectedCategory.id,
            sequence = 1L,
            burstIndex = 0,
        )
}

data class LabCategory(
    val label: String,
    val id: String,
)

sealed interface OutboxLabEffect {
    data class ShowToast(
        val message: String,
    ) : OutboxLabEffect
}

data class LabWriter(
    val label: String,
    val description: String,
    val level: OutboxRecordLevel,
    private val payloadFactory: (
        category: String,
        sequence: Long,
        burstIndex: Int,
    ) -> String,
) {
    fun payload(
        category: String,
        sequence: Long,
        burstIndex: Int,
    ): String {
        return payloadFactory(category, sequence, burstIndex)
    }
}

val LAB_CATEGORIES = listOf(
    LabCategory(
        label = "Network failure",
        id = "demo.network.failure",
    ),
    LabCategory(
        label = "Document sync",
        id = "demo.document.sync",
    ),
    LabCategory(
        label = "Runtime signal",
        id = "demo.runtime.signal",
    ),
    LabCategory(
        label = "Pressure sample",
        id = "demo.outbox.pressure",
    ),
)

val LAB_WRITERS = listOf(
    LabWriter(
        label = "API failure",
        description = "Business-owned network failure payload.",
        level = OutboxRecordLevel.ERROR,
    ) { category, sequence, burstIndex ->
        """
            {"message":"SYNC001 Document sync failed","category":"$category","sequence":$sequence,"burst_index":$burstIndex,"source":"local_editor","target":"cloud_archive","action":"sync_document","failure_type":"api_failure","http_code":500,"path":"/v1/documents/sync"}
        """.trimIndent()
    },
    LabWriter(
        label = "Delivery retry",
        description = "Record remains pending until ACK.",
        level = OutboxRecordLevel.WARN,
    ) { category, sequence, burstIndex ->
        """
            {"message":"Delivery failed; keep record pending","category":"$category","sequence":$sequence,"burst_index":$burstIndex,"transport":"sample_sink","ack_policy":"ack_after_success","expected":"read_again_when_not_acked"}
        """.trimIndent()
    },
    LabWriter(
        label = "Crash breadcrumb",
        description = "Lightweight runtime crash summary.",
        level = OutboxRecordLevel.FATAL,
    ) { category, sequence, burstIndex ->
        """
            {"message":"runtime crash captured for next launch","category":"$category","sequence":$sequence,"burst_index":$burstIndex,"source":"uncaught_exception","fatal":true,"thread":"main","exception":"ArithmeticException","stack_hash":"sample"}
        """.trimIndent()
    },
    LabWriter(
        label = "Outbox pressure",
        description = "Synthetic high-volume burst record.",
        level = OutboxRecordLevel.INFO,
    ) { category, sequence, burstIndex ->
        """
            {"message":"AndroidOutBox lab record","category":"$category","sequence":$sequence,"burst_index":$burstIndex,"timestamp_ms":${System.currentTimeMillis()},"source":"sample_app","delivery":"manual"}
        """.trimIndent()
    },
)

const val LAB_MAX_RECORD_BYTES = 64 * 1024
const val LAB_MAX_BATCH_RECORDS = 32
const val LAB_MAX_BATCH_BYTES = 256 * 1024

internal const val MAX_CONSOLE_LINES = 80
