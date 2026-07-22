package com.android.pt.outbox.ui

import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.ColumnScope
import androidx.compose.foundation.layout.ExperimentalLayoutApi
import androidx.compose.foundation.layout.FlowRow
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Button
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.Card
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.DropdownMenuItem
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.ExposedDropdownMenuAnchorType
import androidx.compose.material3.ExposedDropdownMenuBox
import androidx.compose.material3.ExposedDropdownMenuDefaults
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.material3.TextFieldDefaults
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.TextStyle
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.tooling.preview.Preview
import androidx.compose.ui.unit.dp
import com.android.pt.outbox.LAB_CATEGORIES
import com.android.pt.outbox.LAB_MAX_BATCH_RECORDS
import com.android.pt.outbox.LAB_WRITERS
import com.android.pt.outbox.LabCategory
import com.android.pt.outbox.LabWriter
import com.android.pt.outbox.OutboxLabUiState
import com.android.pt.outbox.ui.theme.AndroidOutBoxTheme
import io.github.phuongtran.androidoutbox.OutboxBatch
import io.github.phuongtran.androidoutbox.OutboxDoorbellEvent
import io.github.phuongtran.androidoutbox.OutboxRecordLevel
import io.github.phuongtran.androidoutbox.OutboxStats
import java.util.Locale

@OptIn(ExperimentalLayoutApi::class)
@Composable
fun OutboxLabScreen(
    state: OutboxLabUiState,
    onCategorySelected: (LabCategory) -> Unit,
    onWriterSelected: (LabWriter) -> Unit,
    onStart: () -> Unit,
    onWriteOne: () -> Unit,
    onBurst: (Int) -> Unit,
    onFlush: () -> Unit,
    onRefreshStats: () -> Unit,
    onReadBatch: () -> Unit,
    onAck: () -> Unit,
    onSimulateFailure: () -> Unit,
    onClearConsole: () -> Unit,
) {
    var isRuntimeHelpVisible by remember { mutableStateOf(false) }

    if (isRuntimeHelpVisible) {
        RuntimeHelpDialog(
            onDismiss = { isRuntimeHelpVisible = false },
        )
    }

    Scaffold(
        modifier = Modifier.fillMaxSize(),
        containerColor = MaterialTheme.colorScheme.background,
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
                Header(stats = state.stats)
            }
            item {
                MetricsPanel(
                    stats = state.stats,
                    lastDoorbellEvent = state.lastDoorbellEvent,
                    doorbellCount = state.doorbellCount,
                    busyAction = state.busyAction,
                    onStart = onStart,
                    onHelpClick = { isRuntimeHelpVisible = true },
                )
            }
            item {
                WriterPanel(
                    selectedCategory = state.selectedCategory,
                    onCategorySelected = onCategorySelected,
                    selectedWriter = state.selectedWriter,
                    onWriterSelected = onWriterSelected,
                    payloadPreview = state.payloadPreview,
                    busyAction = state.busyAction,
                    onWriteOne = onWriteOne,
                    onBurst = onBurst,
                    onFlush = onFlush,
                    onRefreshStats = onRefreshStats,
                )
            }
            item {
                DrainPanel(
                    batch = state.currentBatch,
                    lastDoorbellEvent = state.lastDoorbellEvent,
                    busyAction = state.busyAction,
                    onReadBatch = onReadBatch,
                    onAck = onAck,
                    onSimulateFailure = onSimulateFailure,
                )
            }
            item {
                BatchConsole(batch = state.currentBatch)
            }
            item {
                EventConsole(
                    lines = state.consoleLines,
                    onClear = onClearConsole,
                )
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
                color = MaterialTheme.colorScheme.onBackground,
            )
            Text(
                text = "Native outbox control room",
                style = MaterialTheme.typography.bodyMedium,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
        }
        StatusPill(
            text = if (stats.isStarted) "STARTED" else "STOPPED",
            color = if (stats.isStarted) {
                MaterialTheme.colorScheme.secondary
            } else {
                MaterialTheme.colorScheme.tertiary
            },
        )
    }
}

@OptIn(ExperimentalLayoutApi::class)
@Composable
private fun MetricsPanel(
    stats: OutboxStats,
    lastDoorbellEvent: OutboxDoorbellEvent?,
    doorbellCount: Long,
    busyAction: String?,
    onStart: () -> Unit,
    onHelpClick: () -> Unit,
) {
    Section(
        title = "Runtime",
        trailingContent = {
            TextButton(
                onClick = onHelpClick,
                colors = ButtonDefaults.textButtonColors(
                    contentColor = MaterialTheme.colorScheme.primary,
                ),
            ) {
                Text("Help")
            }
        },
    ) {
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.SpaceBetween,
            verticalAlignment = Alignment.CenterVertically,
        ) {
            Column(
                modifier = Modifier
                    .weight(1f)
                    .padding(end = 12.dp),
            ) {
                Text(
                    text = "Runtime must be started before writer or drain actions.",
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
                Text(
                    text = "Start opens native pipes, configures the spool, and attaches the doorbell listener.",
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
            }
            if (stats.isStarted) {
                StatusPill(
                    text = "ACTIVE",
                    color = MaterialTheme.colorScheme.secondary,
                )
            } else {
                PrimaryActionButton("Start", busyAction, onStart)
            }
        }
        Spacer(modifier = Modifier.height(12.dp))
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
            MetricTile("Doorbell", lastDoorbellEvent?.name ?: "None")
            MetricTile("Signals", doorbellCount.toString())
        }
    }
}

@OptIn(ExperimentalLayoutApi::class)
@Composable
private fun WriterPanel(
    selectedCategory: LabCategory,
    onCategorySelected: (LabCategory) -> Unit,
    selectedWriter: LabWriter,
    onWriterSelected: (LabWriter) -> Unit,
    payloadPreview: String,
    busyAction: String?,
    onWriteOne: () -> Unit,
    onBurst: (Int) -> Unit,
    onFlush: () -> Unit,
    onRefreshStats: () -> Unit,
) {
    Section(title = "Writer") {
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.spacedBy(10.dp),
        ) {
            LabDropdown(
                modifier = Modifier.weight(1f),
                label = "Category",
                selectedLabel = selectedCategory.label,
                items = LAB_CATEGORIES,
                itemLabel = LabCategory::label,
                onItemSelected = onCategorySelected,
            )
            LabDropdown(
                modifier = Modifier.weight(1f),
                label = "Writer",
                selectedLabel = selectedWriter.label,
                items = LAB_WRITERS,
                itemLabel = LabWriter::label,
                onItemSelected = onWriterSelected,
            )
        }
        Spacer(modifier = Modifier.height(12.dp))
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.SpaceBetween,
            verticalAlignment = Alignment.CenterVertically,
        ) {
            Column(
                modifier = Modifier
                    .weight(1f)
                    .padding(end = 12.dp),
            ) {
                Text(
                    text = selectedCategory.id,
                    style = MaterialTheme.typography.bodyMedium,
                    fontFamily = FontFamily.Monospace,
                    color = MaterialTheme.colorScheme.onSurface,
                    maxLines = 1,
                    overflow = TextOverflow.Ellipsis,
                )
                Text(
                    text = selectedWriter.description,
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    maxLines = 2,
                    overflow = TextOverflow.Ellipsis,
                )
            }
            StatusPill(
                text = selectedWriter.level.name,
                color = when (selectedWriter.level) {
                    OutboxRecordLevel.ERROR,
                    OutboxRecordLevel.FATAL,
                    OutboxRecordLevel.WARN,
                    -> MaterialTheme.colorScheme.tertiary
                    else -> MaterialTheme.colorScheme.primary
                },
            )
        }
        Spacer(modifier = Modifier.height(12.dp))
        OutlinedTextField(
            modifier = Modifier.fillMaxWidth(),
            value = payloadPreview,
            onValueChange = {},
            readOnly = true,
            label = { Text("Generated payload preview") },
            minLines = 5,
            maxLines = 7,
            textStyle = TextStyle(
                color = MaterialTheme.colorScheme.onSurface,
                fontFamily = FontFamily.Monospace,
            ),
            colors = labTextFieldColors(),
        )
        Spacer(modifier = Modifier.height(10.dp))
        FlowRow(
            horizontalArrangement = Arrangement.spacedBy(8.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp),
        ) {
            SecondaryActionButton("Write 1", busyAction, onWriteOne)
            SecondaryActionButton("Burst 100", busyAction) { onBurst(100) }
            SecondaryActionButton("Burst 1000", busyAction) { onBurst(1000) }
            SecondaryActionButton("Flush", busyAction, onFlush)
            TextButton(
                onClick = onRefreshStats,
                colors = ButtonDefaults.textButtonColors(
                    contentColor = MaterialTheme.colorScheme.primary,
                ),
            ) {
                Text("Refresh stats")
            }
        }
    }
}

@OptIn(ExperimentalLayoutApi::class)
@Composable
private fun DrainPanel(
    batch: OutboxBatch?,
    lastDoorbellEvent: OutboxDoorbellEvent?,
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
                text = "Loaded batch",
                style = MaterialTheme.typography.titleMedium,
                color = MaterialTheme.colorScheme.onSurface,
            )
            StatusPill(
                text = "${batch?.records?.size ?: 0} LOADED",
                color = if (batch == null) {
                    MaterialTheme.colorScheme.outline
                } else {
                    MaterialTheme.colorScheme.primary
                },
            )
        }
        Spacer(modifier = Modifier.height(12.dp))
        StatusPill(
            text = if (lastDoorbellEvent == OutboxDoorbellEvent.DATA_AVAILABLE) {
                "AUTO DRAIN ON DOORBELL"
            } else {
                "WAITING FOR DOORBELL"
            },
            color = if (lastDoorbellEvent == OutboxDoorbellEvent.DATA_AVAILABLE) {
                MaterialTheme.colorScheme.secondary
            } else {
                MaterialTheme.colorScheme.outline
            },
        )
        Spacer(modifier = Modifier.height(12.dp))
        Text(
            text = "Doorbell is the wake-up signal. When native reports DATA_AVAILABLE, Kotlin auto-pulls up to $LAB_MAX_BATCH_RECORDS durable records without moving the cursor.",
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
        Spacer(modifier = Modifier.height(6.dp))
        Text(
            text = "ACK commits only the loaded batch after delivery succeeds. Manual read is kept here as a debug fallback.",
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
        Spacer(modifier = Modifier.height(12.dp))
        FlowRow(
            horizontalArrangement = Arrangement.spacedBy(8.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp),
        ) {
            PrimaryActionButton("Manual read", busyAction, onReadBatch)
            SecondaryActionButton("ACK", busyAction, onAck)
            OutlinedButton(
                onClick = onSimulateFailure,
                colors = ButtonDefaults.outlinedButtonColors(
                    contentColor = MaterialTheme.colorScheme.tertiary,
                ),
            ) {
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
private fun EventConsole(
    lines: List<String>,
    onClear: () -> Unit,
) {
    Section(
        title = "Event Console",
        trailingContent = {
            TextButton(
                enabled = lines.isNotEmpty(),
                onClick = onClear,
                colors = ButtonDefaults.textButtonColors(
                    contentColor = MaterialTheme.colorScheme.primary,
                    disabledContentColor = MaterialTheme.colorScheme.onSurfaceVariant,
                ),
            ) {
                Text("Clear")
            }
        },
    ) {
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
    trailingContent: (@Composable () -> Unit)? = null,
    content: @Composable ColumnScope.() -> Unit,
) {
    Card(
        modifier = Modifier.fillMaxWidth(),
        shape = RoundedCornerShape(8.dp),
        colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surface),
        elevation = CardDefaults.cardElevation(defaultElevation = 0.dp),
    ) {
        Column(
            modifier = Modifier.padding(14.dp),
        ) {
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween,
                verticalAlignment = Alignment.CenterVertically,
            ) {
                Text(
                    text = title,
                    style = MaterialTheme.typography.titleMedium,
                    fontWeight = FontWeight.SemiBold,
                    color = MaterialTheme.colorScheme.onSurface,
                )
                trailingContent?.invoke()
            }
            Spacer(modifier = Modifier.height(12.dp))
            content()
        }
    }
}

@Composable
private fun RuntimeHelpDialog(
    onDismiss: () -> Unit,
) {
    AlertDialog(
        onDismissRequest = onDismiss,
        confirmButton = {
            TextButton(
                onClick = onDismiss,
                colors = ButtonDefaults.textButtonColors(
                    contentColor = MaterialTheme.colorScheme.primary,
                ),
            ) {
                Text("Got it")
            }
        },
        title = {
            Text(
                text = "How the outbox flow works",
                color = MaterialTheme.colorScheme.onSurface,
            )
        },
        text = {
            Column(
                modifier = Modifier
                    .fillMaxWidth()
                    .height(360.dp)
                    .verticalScroll(rememberScrollState()),
                verticalArrangement = Arrangement.spacedBy(12.dp),
            ) {
                HelpStep(
                    title = "1. Start",
                    body = "Opens the native outbox and prepares the file-first spool. Nothing is sent to a backend here.",
                )
                HelpStep(
                    title = "2. Write",
                    body = "Writes one record into the bounded native outbox. The call site hands the payload off quickly; native persists it and rings DATA_AVAILABLE when Kotlin should wake up.",
                )
                HelpStep(
                    title = "3. Flush",
                    body = "Asks native to finish writing queued records to disk. This is useful in the demo after a burst, but the normal drain still starts from doorbell.",
                )
                HelpStep(
                    title = "4. Doorbell read",
                    body = "Kotlin listens to native doorbells. When DATA_AVAILABLE arrives, the demo auto-pulls up to $LAB_MAX_BATCH_RECORDS pending durable records without advancing the native cursor.",
                )
                HelpStep(
                    title = "5. Deliver",
                    body = "In a real app, Kotlin would POST this batch to Sentry, Loki, or any app-owned sink. This sample only shows the batch and lets you simulate success or failure.",
                )
                HelpStep(
                    title = "6. ACK",
                    body = "ACK means delivery succeeded for the loaded batch. Native can safely advance the cursor and ring another doorbell if more records remain.",
                )
                HelpStep(
                    title = "Failure path",
                    body = "If delivery fails, do not ACK. The cursor stays where it is, and a later manual retry can return the same pending records.",
                )
                HelpStep(
                    title = "Manual read",
                    body = "The button is kept only for debugging or retry experiments. The intended flow is doorbell first, then read, then deliver, then ACK.",
                )
            }
        },
        containerColor = MaterialTheme.colorScheme.surface,
        titleContentColor = MaterialTheme.colorScheme.onSurface,
        textContentColor = MaterialTheme.colorScheme.onSurfaceVariant,
    )
}

@Composable
private fun HelpStep(
    title: String,
    body: String,
) {
    Column(
        verticalArrangement = Arrangement.spacedBy(4.dp),
    ) {
        Text(
            text = title,
            style = MaterialTheme.typography.titleSmall,
            fontWeight = FontWeight.SemiBold,
            color = MaterialTheme.colorScheme.onSurface,
        )
        Text(
            text = body,
            style = MaterialTheme.typography.bodyMedium,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
    }
}

@Composable
private fun MetricTile(
    label: String,
    value: String,
) {
    Surface(
        shape = RoundedCornerShape(8.dp),
        color = MaterialTheme.colorScheme.surfaceVariant,
    ) {
        Column(
            modifier = Modifier
                .width(104.dp)
                .padding(horizontal = 10.dp, vertical = 8.dp),
        ) {
            Text(
                text = label,
                style = MaterialTheme.typography.labelMedium,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                maxLines = 1,
            )
            Text(
                text = value,
                style = MaterialTheme.typography.titleMedium,
                fontWeight = FontWeight.Bold,
                color = MaterialTheme.colorScheme.onSurface,
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
        color = color.copy(alpha = 0.16f),
    ) {
        Text(
            modifier = Modifier.padding(horizontal = 10.dp, vertical = 6.dp),
            text = text,
            style = MaterialTheme.typography.labelMedium,
            fontWeight = FontWeight.Bold,
            color = color,
            maxLines = 1,
            softWrap = false,
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
                color = MaterialTheme.colorScheme.inverseSurface,
                shape = RoundedCornerShape(6.dp),
            )
            .padding(8.dp),
        verticalAlignment = Alignment.Top,
    ) {
        Text(
            text = prefix,
            style = MaterialTheme.typography.labelMedium,
            fontFamily = FontFamily.Monospace,
            color = MaterialTheme.colorScheme.primary,
            maxLines = 1,
        )
        Spacer(modifier = Modifier.width(10.dp))
        Text(
            text = text,
            style = MaterialTheme.typography.bodySmall,
            fontFamily = FontFamily.Monospace,
            color = MaterialTheme.colorScheme.onSurface,
        )
    }
}

@Composable
private fun EmptyLine(text: String) {
    Text(
        modifier = Modifier
            .fillMaxWidth()
            .background(
                color = MaterialTheme.colorScheme.surfaceVariant,
                shape = RoundedCornerShape(8.dp),
            )
            .padding(12.dp),
        text = text,
        style = MaterialTheme.typography.bodyMedium,
        color = MaterialTheme.colorScheme.onSurfaceVariant,
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
        colors = ButtonDefaults.buttonColors(
            containerColor = MaterialTheme.colorScheme.primary,
            contentColor = MaterialTheme.colorScheme.onPrimary,
            disabledContainerColor = MaterialTheme.colorScheme.surfaceVariant,
            disabledContentColor = MaterialTheme.colorScheme.onSurfaceVariant,
        ),
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
        colors = ButtonDefaults.outlinedButtonColors(
            contentColor = MaterialTheme.colorScheme.onSurface,
            disabledContentColor = MaterialTheme.colorScheme.onSurfaceVariant,
        ),
    ) {
        Text(if (busyAction == label) "Running" else label)
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun <T> LabDropdown(
    modifier: Modifier = Modifier,
    label: String,
    selectedLabel: String,
    items: List<T>,
    itemLabel: (T) -> String,
    onItemSelected: (T) -> Unit,
) {
    var expanded by remember { mutableStateOf(false) }
    ExposedDropdownMenuBox(
        modifier = modifier,
        expanded = expanded,
        onExpandedChange = { expanded = !expanded },
    ) {
        OutlinedTextField(
            modifier = Modifier
                .menuAnchor(
                    type = ExposedDropdownMenuAnchorType.PrimaryNotEditable,
                    enabled = true,
                )
                .fillMaxWidth(),
            value = selectedLabel,
            onValueChange = {},
            readOnly = true,
            singleLine = true,
            label = { Text(label) },
            trailingIcon = {
                ExposedDropdownMenuDefaults.TrailingIcon(expanded = expanded)
            },
            textStyle = TextStyle(color = MaterialTheme.colorScheme.onSurface),
            colors = labTextFieldColors(),
        )
        ExposedDropdownMenu(
            expanded = expanded,
            onDismissRequest = { expanded = false },
            containerColor = MaterialTheme.colorScheme.surface,
        ) {
            items.forEach { item ->
                DropdownMenuItem(
                    text = {
                        Text(
                            text = itemLabel(item),
                            color = MaterialTheme.colorScheme.onSurface,
                        )
                    },
                    onClick = {
                        expanded = false
                        onItemSelected(item)
                    },
                )
            }
        }
    }
}

@Composable
private fun labTextFieldColors() = TextFieldDefaults.colors(
    focusedTextColor = MaterialTheme.colorScheme.onSurface,
    unfocusedTextColor = MaterialTheme.colorScheme.onSurface,
    focusedContainerColor = MaterialTheme.colorScheme.surfaceVariant,
    unfocusedContainerColor = MaterialTheme.colorScheme.surfaceVariant,
    disabledContainerColor = MaterialTheme.colorScheme.surfaceVariant,
    focusedIndicatorColor = MaterialTheme.colorScheme.primary,
    unfocusedIndicatorColor = MaterialTheme.colorScheme.outline,
    focusedLabelColor = MaterialTheme.colorScheme.primary,
    unfocusedLabelColor = MaterialTheme.colorScheme.onSurfaceVariant,
    cursorColor = MaterialTheme.colorScheme.primary,
)

private fun OutboxStats.totalDropped(): Long {
    return droppedQueueFullCount + droppedInvalidCount + droppedRecordTooLargeCount
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

@Preview(showBackground = true)
@Composable
private fun OutboxLabPreview() {
    AndroidOutBoxTheme(
        darkTheme = true,
        dynamicColor = false,
    ) {
        OutboxLabScreen(
            state = OutboxLabUiState(
                stats = OutboxStats(
                    isStarted = true,
                    queueCapacity = 256,
                    queueDepth = 3,
                    queueHighWatermark = 12,
                    acceptedCount = 120,
                    writtenCount = 117,
                    currentFileSizeBytes = 14_400,
                ),
            ),
            onCategorySelected = {},
            onWriterSelected = {},
            onStart = {},
            onWriteOne = {},
            onBurst = {},
            onFlush = {},
            onRefreshStats = {},
            onReadBatch = {},
            onAck = {},
            onSimulateFailure = {},
            onClearConsole = {},
        )
    }
}
