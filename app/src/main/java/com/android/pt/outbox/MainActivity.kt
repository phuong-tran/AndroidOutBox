package com.android.pt.outbox

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.ExperimentalLayoutApi
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.ColumnScope
import androidx.compose.foundation.layout.FlowRow
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Button
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.Card
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.FilterChip
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateListOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.tooling.preview.Preview
import androidx.compose.ui.unit.dp
import com.android.pt.outbox.ui.theme.AndroidOutBoxTheme
import io.github.phuongtran.androidoutbox.AndroidOutbox
import io.github.phuongtran.androidoutbox.AndroidOutboxFactory
import io.github.phuongtran.androidoutbox.OutboxBatch
import io.github.phuongtran.androidoutbox.OutboxConfig
import io.github.phuongtran.androidoutbox.OutboxRecordLevel
import io.github.phuongtran.androidoutbox.OutboxStats
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.io.File
import java.util.Locale

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()
        setContent {
            AndroidOutBoxTheme(dynamicColor = false) {
                OutboxLabScreen()
            }
        }
    }
}

@OptIn(ExperimentalLayoutApi::class)
@Composable
private fun OutboxLabScreen(
    outbox: AndroidOutbox = remember { AndroidOutboxFactory.create() },
) {
    val context = LocalContext.current
    val scope = rememberCoroutineScope()
    val console = remember { mutableStateListOf<String>() }
    var stats by remember { mutableStateOf(OutboxStats()) }
    var currentBatch by remember { mutableStateOf<OutboxBatch?>(null) }
    var selectedLevel by remember { mutableStateOf(OutboxRecordLevel.INFO) }
    var category by remember { mutableStateOf("demo.network_error") }
    var payload by remember {
        mutableStateOf(
            """
                {"message":"ERR0001 Add to cart failed","source":"sample_app","target":"outbox_lab","http_code":500}
            """.trimIndent(),
        )
    }
    var busyAction by remember { mutableStateOf<String?>(null) }

    fun appendConsole(message: String) {
        console.add(0, "${System.currentTimeMillis()}  $message")
        while (console.size > MAX_CONSOLE_LINES) {
            console.removeAt(console.lastIndex)
        }
    }

    fun refreshStats() {
        scope.launch {
            stats = withContext(Dispatchers.IO) {
                outbox.getStats()
            }
        }
    }

    fun runAction(
        name: String,
        action: suspend () -> String,
    ) {
        scope.launch {
            busyAction = name
            val result = runCatching {
                withContext(Dispatchers.IO) {
                    action()
                }
            }.getOrElse { throwable ->
                "${name} failed: ${throwable.javaClass.simpleName}"
            }
            appendConsole(result)
            stats = withContext(Dispatchers.IO) {
                outbox.getStats()
            }
            busyAction = null
        }
    }

    fun readBatch() {
        scope.launch {
            busyAction = "Read batch"
            val batch = withContext(Dispatchers.IO) {
                outbox.readNextBatch(
                    maxRecords = LAB_MAX_BATCH_RECORDS,
                    maxBytes = LAB_MAX_BATCH_BYTES,
                )
            }
            currentBatch = batch
            appendConsole("read batch records=${batch?.records?.size ?: 0}")
            stats = withContext(Dispatchers.IO) {
                outbox.getStats()
            }
            busyAction = null
        }
    }

    fun ackBatch() {
        val batch = currentBatch
        scope.launch {
            busyAction = "ACK"
            val ok = if (batch == null) {
                false
            } else {
                withContext(Dispatchers.IO) {
                    outbox.ack(ackToken = batch.ackToken)
                }
            }
            if (ok) {
                currentBatch = null
            }
            appendConsole(
                if (batch == null) {
                    "ack skipped: no batch"
                } else {
                    "ack ok=$ok records=${batch.records.size}"
                },
            )
            stats = withContext(Dispatchers.IO) {
                outbox.getStats()
            }
            busyAction = null
        }
    }

    DisposableEffect(outbox) {
        onDispose {
            runCatching {
                outbox.stop()
            }
        }
    }

    Scaffold(
        modifier = Modifier.fillMaxSize(),
        containerColor = LabColors.Background,
    ) { innerPadding ->
        LazyColumn(
            modifier = Modifier
                .fillMaxSize()
                .padding(innerPadding)
                .padding(horizontal = 16.dp),
            verticalArrangement = Arrangement.spacedBy(12.dp),
        ) {
            item {
                Spacer(modifier = Modifier.height(8.dp))
                Header(stats = stats)
            }
            item {
                MetricsPanel(stats = stats)
            }
            item {
                WriterPanel(
                    category = category,
                    onCategoryChange = { category = it },
                    payload = payload,
                    onPayloadChange = { payload = it },
                    selectedLevel = selectedLevel,
                    onLevelSelected = { selectedLevel = it },
                    busyAction = busyAction,
                    onStart = {
                        runAction("Start") {
                            val config = OutboxConfig(
                                spoolDirectoryPath = File(
                                    context.cacheDir,
                                    "android-outbox-lab",
                                ).absolutePath,
                                maxRecordBytes = LAB_MAX_RECORD_BYTES,
                            )
                            if (outbox.start(config) || outbox.getStats().isStarted) {
                                "started spool=${config.spoolDirectoryPath}"
                            } else {
                                "start returned false"
                            }
                        }
                    },
                    onWriteOne = {
                        runAction("Write 1") {
                            val ok = outbox.write(
                                level = selectedLevel,
                                category = category.trim(),
                                payload = payload.trim(),
                            )
                            "write one ok=$ok"
                        }
                    },
                    onBurst = { count ->
                        runAction("Burst $count") {
                            var accepted = 0
                            repeat(count) { index ->
                                if (outbox.write(
                                        level = selectedLevel,
                                        category = category.trim(),
                                        payload = generatedPayload(
                                            category = category.trim(),
                                            sequence = System.currentTimeMillis() + index,
                                            burstIndex = index,
                                        ),
                                    )
                                ) {
                                    accepted += 1
                                }
                            }
                            "burst requested=$count accepted=$accepted"
                        }
                    },
                    onFlush = {
                        runAction("Flush") {
                            "flush ok=${outbox.flush()}"
                        }
                    },
                    onRefreshStats = ::refreshStats,
                )
            }
            item {
                DrainPanel(
                    batch = currentBatch,
                    busyAction = busyAction,
                    onReadBatch = ::readBatch,
                    onAck = ::ackBatch,
                    onSimulateFailure = {
                        val count = currentBatch?.records?.size ?: 0
                        appendConsole("simulated delivery failure: no ACK records=$count")
                    },
                )
            }
            item {
                BatchConsole(batch = currentBatch)
            }
            item {
                EventConsole(lines = console)
                Spacer(modifier = Modifier.height(16.dp))
            }
        }
    }
}

@Composable
private fun Header(stats: OutboxStats) {
    Row(
        modifier = Modifier.fillMaxWidth(),
        horizontalArrangement = Arrangement.SpaceBetween,
        verticalAlignment = Alignment.CenterVertically,
    ) {
        Column {
            Text(
                text = "AndroidOutBox Lab",
                style = MaterialTheme.typography.headlineSmall,
                fontWeight = FontWeight.Bold,
                color = LabColors.TextStrong,
            )
            Text(
                text = "Maven AAR playground",
                style = MaterialTheme.typography.bodyMedium,
                color = LabColors.TextMuted,
            )
        }
        StatusPill(
            text = if (stats.isStarted) "STARTED" else "STOPPED",
            color = if (stats.isStarted) LabColors.Success else LabColors.Warning,
        )
    }
}

@OptIn(ExperimentalLayoutApi::class)
@Composable
private fun MetricsPanel(stats: OutboxStats) {
    Section(title = "Runtime") {
        FlowRow(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.spacedBy(8.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp),
        ) {
            MetricTile("Queue", "${stats.queueDepth}/${stats.queueCapacity}")
            MetricTile("High", stats.queueHighWatermark.toString())
            MetricTile("Accepted", stats.acceptedCount.toString())
            MetricTile("Written", stats.writtenCount.toString())
            MetricTile("Dropped", stats.totalDropped().toString())
            MetricTile("File", formatBytes(stats.currentFileSizeBytes))
            MetricTile("Rolls", stats.rollCount.toString())
        }
    }
}

@OptIn(ExperimentalLayoutApi::class)
@Composable
private fun WriterPanel(
    category: String,
    onCategoryChange: (String) -> Unit,
    payload: String,
    onPayloadChange: (String) -> Unit,
    selectedLevel: OutboxRecordLevel,
    onLevelSelected: (OutboxRecordLevel) -> Unit,
    busyAction: String?,
    onStart: () -> Unit,
    onWriteOne: () -> Unit,
    onBurst: (Int) -> Unit,
    onFlush: () -> Unit,
    onRefreshStats: () -> Unit,
) {
    Section(title = "Writer") {
        FlowRow(
            horizontalArrangement = Arrangement.spacedBy(8.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp),
        ) {
            OutboxRecordLevel.entries.forEach { level ->
                FilterChip(
                    selected = selectedLevel == level,
                    onClick = { onLevelSelected(level) },
                    label = { Text(level.name) },
                )
            }
        }
        Spacer(modifier = Modifier.height(10.dp))
        OutlinedTextField(
            modifier = Modifier.fillMaxWidth(),
            value = category,
            onValueChange = onCategoryChange,
            label = { Text("Category") },
            singleLine = true,
        )
        Spacer(modifier = Modifier.height(10.dp))
        OutlinedTextField(
            modifier = Modifier.fillMaxWidth(),
            value = payload,
            onValueChange = onPayloadChange,
            label = { Text("Payload") },
            minLines = 4,
            maxLines = 6,
        )
        Spacer(modifier = Modifier.height(12.dp))
        FlowRow(
            horizontalArrangement = Arrangement.spacedBy(8.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp),
        ) {
            PrimaryActionButton("Start", busyAction, onStart)
            SecondaryActionButton("Write 1", busyAction, onWriteOne)
            SecondaryActionButton("Burst 100", busyAction) { onBurst(100) }
            SecondaryActionButton("Burst 1000", busyAction) { onBurst(1000) }
            SecondaryActionButton("Flush", busyAction, onFlush)
            TextButton(onClick = onRefreshStats) {
                Text("Refresh stats")
            }
        }
    }
}

@OptIn(ExperimentalLayoutApi::class)
@Composable
private fun DrainPanel(
    batch: OutboxBatch?,
    busyAction: String?,
    onReadBatch: () -> Unit,
    onAck: () -> Unit,
    onSimulateFailure: () -> Unit,
) {
    Section(title = "Drain") {
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.SpaceBetween,
            verticalAlignment = Alignment.CenterVertically,
        ) {
            Text(
                text = "Pending batch",
                style = MaterialTheme.typography.titleMedium,
                color = LabColors.TextStrong,
            )
            StatusPill(
                text = "${batch?.records?.size ?: 0} RECORDS",
                color = if (batch == null) LabColors.Neutral else LabColors.Info,
            )
        }
        Spacer(modifier = Modifier.height(12.dp))
        FlowRow(
            horizontalArrangement = Arrangement.spacedBy(8.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp),
        ) {
            PrimaryActionButton("Read batch", busyAction, onReadBatch)
            SecondaryActionButton("ACK", busyAction, onAck)
            OutlinedButton(onClick = onSimulateFailure) {
                Text("Simulate failure")
            }
        }
    }
}

@Composable
private fun BatchConsole(batch: OutboxBatch?) {
    Section(title = "Batch Records") {
        val records = batch?.records.orEmpty()
        if (records.isEmpty()) {
            EmptyLine("No batch loaded")
        } else {
            Column(
                modifier = Modifier
                    .fillMaxWidth()
                    .height(220.dp)
                    .verticalScroll(rememberScrollState()),
                verticalArrangement = Arrangement.spacedBy(8.dp),
            ) {
                records.forEachIndexed { index, record ->
                    ConsoleLine(
                        prefix = "#${index + 1}",
                        text = record,
                    )
                }
            }
        }
    }
}

@Composable
private fun EventConsole(lines: List<String>) {
    Section(title = "Event Console") {
        if (lines.isEmpty()) {
            EmptyLine("No events yet")
        } else {
            Column(
                modifier = Modifier
                    .fillMaxWidth()
                    .height(180.dp)
                    .verticalScroll(rememberScrollState()),
                verticalArrangement = Arrangement.spacedBy(6.dp),
            ) {
                lines.forEach { line ->
                    ConsoleLine(
                        prefix = "lab",
                        text = line,
                    )
                }
            }
        }
    }
}

@Composable
private fun Section(
    title: String,
    content: @Composable ColumnScope.() -> Unit,
) {
    Card(
        modifier = Modifier.fillMaxWidth(),
        shape = RoundedCornerShape(8.dp),
        colors = CardDefaults.cardColors(containerColor = LabColors.Surface),
        elevation = CardDefaults.cardElevation(defaultElevation = 1.dp),
    ) {
        Column(
            modifier = Modifier.padding(14.dp),
        ) {
            Text(
                text = title,
                style = MaterialTheme.typography.titleMedium,
                fontWeight = FontWeight.SemiBold,
                color = LabColors.TextStrong,
            )
            Spacer(modifier = Modifier.height(12.dp))
            content()
        }
    }
}

@Composable
private fun MetricTile(
    label: String,
    value: String,
) {
    Surface(
        shape = RoundedCornerShape(8.dp),
        color = LabColors.Tile,
    ) {
        Column(
            modifier = Modifier
                .width(104.dp)
                .padding(horizontal = 10.dp, vertical = 8.dp),
        ) {
            Text(
                text = label,
                style = MaterialTheme.typography.labelMedium,
                color = LabColors.TextMuted,
                maxLines = 1,
            )
            Text(
                text = value,
                style = MaterialTheme.typography.titleMedium,
                fontWeight = FontWeight.Bold,
                color = LabColors.TextStrong,
                maxLines = 1,
                overflow = TextOverflow.Ellipsis,
            )
        }
    }
}

@Composable
private fun StatusPill(
    text: String,
    color: Color,
) {
    Surface(
        shape = RoundedCornerShape(8.dp),
        color = color.copy(alpha = 0.14f),
    ) {
        Text(
            modifier = Modifier.padding(horizontal = 10.dp, vertical = 6.dp),
            text = text,
            style = MaterialTheme.typography.labelMedium,
            fontWeight = FontWeight.Bold,
            color = color,
        )
    }
}

@Composable
private fun ConsoleLine(
    prefix: String,
    text: String,
) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .background(
                color = LabColors.CodeSurface,
                shape = RoundedCornerShape(6.dp),
            )
            .padding(8.dp),
        verticalAlignment = Alignment.Top,
    ) {
        Text(
            text = prefix,
            style = MaterialTheme.typography.labelMedium,
            fontFamily = FontFamily.Monospace,
            color = LabColors.Info,
            maxLines = 1,
        )
        Spacer(modifier = Modifier.width(10.dp))
        Text(
            text = text,
            style = MaterialTheme.typography.bodySmall,
            fontFamily = FontFamily.Monospace,
            color = LabColors.TextStrong,
        )
    }
}

@Composable
private fun EmptyLine(text: String) {
    Text(
        modifier = Modifier
            .fillMaxWidth()
            .background(
                color = LabColors.Tile,
                shape = RoundedCornerShape(8.dp),
            )
            .padding(12.dp),
        text = text,
        style = MaterialTheme.typography.bodyMedium,
        color = LabColors.TextMuted,
    )
}

@Composable
private fun PrimaryActionButton(
    label: String,
    busyAction: String?,
    onClick: () -> Unit,
) {
    Button(
        enabled = busyAction == null,
        onClick = onClick,
        colors = ButtonDefaults.buttonColors(containerColor = LabColors.Info),
    ) {
        Text(if (busyAction == label) "Running" else label)
    }
}

@Composable
private fun SecondaryActionButton(
    label: String,
    busyAction: String?,
    onClick: () -> Unit,
) {
    OutlinedButton(
        enabled = busyAction == null,
        onClick = onClick,
    ) {
        Text(if (busyAction == label) "Running" else label)
    }
}

private fun OutboxStats.totalDropped(): Long {
    return droppedQueueFullCount + droppedInvalidCount + droppedRecordTooLargeCount
}

private fun generatedPayload(
    category: String,
    sequence: Long,
    burstIndex: Int,
): String {
    return """
        {"message":"AndroidOutBox lab record","category":"$category","sequence":$sequence,"burst_index":$burstIndex,"timestamp_ms":${System.currentTimeMillis()},"source":"sample_app","delivery":"manual"}
    """.trimIndent()
}

private fun formatBytes(bytes: Long): String {
    if (bytes < 1024L) {
        return "${bytes}B"
    }
    val kib = bytes / 1024.0
    if (kib < 1024.0) {
        return String.format(Locale.US, "%.1fKiB", kib)
    }
    return String.format(Locale.US, "%.1fMiB", kib / 1024.0)
}

private object LabColors {
    val Background = Color(0xFFF5F7F9)
    val Surface = Color.White
    val Tile = Color(0xFFEAF0F2)
    val CodeSurface = Color(0xFFF0F4F3)
    val TextStrong = Color(0xFF172326)
    val TextMuted = Color(0xFF607276)
    val Info = Color(0xFF176B87)
    val Success = Color(0xFF168A5B)
    val Warning = Color(0xFFB26A00)
    val Neutral = Color(0xFF68777A)
}

private const val MAX_CONSOLE_LINES = 80
private const val LAB_MAX_RECORD_BYTES = 64 * 1024
private const val LAB_MAX_BATCH_RECORDS = 32
private const val LAB_MAX_BATCH_BYTES = 256 * 1024

@Preview(showBackground = true)
@Composable
private fun OutboxLabPreview() {
    AndroidOutBoxTheme(dynamicColor = false) {
        OutboxLabScreen(outbox = PreviewOutbox)
    }
}

private object PreviewOutbox : AndroidOutbox {
    override fun start(config: OutboxConfig): Boolean = true

    override fun write(
        level: OutboxRecordLevel,
        category: String,
        payload: String,
    ): Boolean = true

    override fun flush(): Boolean = true

    override fun stop() = Unit

    override fun getStats(): OutboxStats {
        return OutboxStats(
            isStarted = true,
            queueCapacity = 256,
            queueDepth = 3,
            queueHighWatermark = 12,
            acceptedCount = 120,
            writtenCount = 117,
            currentFileSizeBytes = 14_400,
        )
    }

    override fun readNextDoorbell() = null

    override fun readNextBatch(
        providerId: String,
        maxRecords: Int,
        maxBytes: Int,
    ): OutboxBatch {
        return OutboxBatch(
            ackToken = byteArrayOf(1, 2, 3),
            records = listOf("preview record"),
        )
    }

    override fun ack(
        providerId: String,
        ackToken: ByteArray,
    ): Boolean = true
}
