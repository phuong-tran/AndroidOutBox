package com.android.pt.outbox

import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import io.github.phuongtran.androidoutbox.AndroidOutbox
import io.github.phuongtran.androidoutbox.AndroidOutboxFactory
import io.github.phuongtran.androidoutbox.BlockingOutboxDoorbellChannel
import io.github.phuongtran.androidoutbox.OutboxConfig
import io.github.phuongtran.androidoutbox.OutboxDoorbellEvent
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.io.File

class OutboxLabViewModel(
    application: Application,
) : AndroidViewModel(application) {

    private val outbox: AndroidOutbox = AndroidOutboxFactory.create()
    private val doorbells = BlockingOutboxDoorbellChannel(outbox)
    private var doorbellJob: Job? = null

    private val _uiState = MutableStateFlow(OutboxLabUiState())
    val uiState: StateFlow<OutboxLabUiState> = _uiState.asStateFlow()

    private val _effects = MutableSharedFlow<OutboxLabEffect>(
        extraBufferCapacity = 1,
    )
    val effects: SharedFlow<OutboxLabEffect> = _effects.asSharedFlow()

    fun selectCategory(category: LabCategory) {
        _uiState.update {
            it.copy(selectedCategory = category)
        }
    }

    fun selectWriter(writer: LabWriter) {
        _uiState.update {
            it.copy(selectedWriter = writer)
        }
    }

    fun startOutbox() {
        runAction(
            name = "Start",
            nextStep = "Doorbell listener is active. Write records and wait for native DATA_AVAILABLE.",
        ) {
            val config = OutboxConfig(
                spoolDirectoryPath = File(
                    getApplication<Application>().cacheDir,
                    "android-outbox-lab",
                ).absolutePath,
                maxRecordBytes = LAB_MAX_RECORD_BYTES,
            )
            if (outbox.start(config) || outbox.getStats().isStarted) {
                startDoorbellListener()
                "started spool=${config.spoolDirectoryPath}"
            } else {
                "start returned false"
            }
        }
    }

    fun writeOne() {
        if (!requireStarted("Write 1")) {
            return
        }
        val state = _uiState.value
        runAction(
            name = "Write 1",
            nextStep = "Record written. Native will ring the doorbell; Kotlin will auto-read a pending batch.",
        ) {
            val ok = outbox.write(
                level = state.selectedWriter.level,
                category = state.selectedCategory.id,
                payload = state.selectedWriter.payload(
                    category = state.selectedCategory.id,
                    sequence = System.currentTimeMillis(),
                    burstIndex = 0,
                ),
            )
            "write one ok=$ok writer=${state.selectedWriter.label} category=${state.selectedCategory.id}"
        }
    }

    fun burst(count: Int) {
        if (!requireStarted("Burst $count")) {
            return
        }
        val state = _uiState.value
        runAction(
            name = "Burst $count",
            nextStep = "Burst queued. Flush if you want to force persistence, then wait for the doorbell auto-read.",
        ) {
            var accepted = 0
            repeat(count) { index ->
                if (outbox.write(
                        level = state.selectedWriter.level,
                        category = state.selectedCategory.id,
                        payload = state.selectedWriter.payload(
                            category = state.selectedCategory.id,
                            sequence = System.currentTimeMillis() + index,
                            burstIndex = index,
                        ),
                    )
                ) {
                    accepted += 1
                }
            }
            "burst requested=$count accepted=$accepted writer=${state.selectedWriter.label}"
        }
    }

    fun flush() {
        if (!requireStarted("Flush")) {
            return
        }
        runAction(
            name = "Flush",
            nextStep = "Flush requested. Doorbell-driven drain will load a batch when native reports data.",
        ) {
            "flush ok=${outbox.flush()}"
        }
    }

    fun refreshStats() {
        viewModelScope.launch {
            val stats = readStats()
            _uiState.update {
                it.copy(stats = stats)
            }
        }
    }

    fun clearConsole() {
        _uiState.update {
            it.copy(consoleLines = emptyList())
        }
    }

    fun readBatch() {
        if (!requireStarted("Manual read")) {
            return
        }
        readBatch(
            trigger = "manual",
            showToast = true,
        )
    }

    private fun readBatch(
        trigger: String,
        showToast: Boolean,
    ) {
        viewModelScope.launch {
            readBatchInternal(
                trigger = trigger,
                showToast = showToast,
            )
        }
    }

    fun ackBatch() {
        if (!requireStarted("ACK")) {
            return
        }
        val batch = _uiState.value.currentBatch
        viewModelScope.launch {
            _uiState.update {
                it.copy(busyAction = "ACK")
            }
            val ok = if (batch == null) {
                false
            } else {
                withContext(Dispatchers.IO) {
                    outbox.ack(ackToken = batch.ackToken)
                }
            }
            val stats = readStats()
            _uiState.update {
                it.copy(
                    currentBatch = if (ok) null else it.currentBatch,
                    stats = stats,
                    busyAction = null,
                    consoleLines = appendConsoleLine(
                        lines = it.consoleLines,
                        message = if (batch == null) {
                            "ack skipped: no batch"
                        } else {
                            "ack ok=$ok records=${batch.records.size}"
                        },
                    ),
                )
            }
            _effects.emit(
                OutboxLabEffect.ShowToast(
                    message = if (ok) {
                        "ACK committed. Loaded batch cleared. If native still has pending records, the next doorbell will load another batch."
                    } else {
                        "ACK skipped. Read a batch first."
                    },
                ),
            )
        }
    }

    fun simulateFailure() {
        if (!requireStarted("Simulate failure")) {
            return
        }
        val count = _uiState.value.currentBatch?.records?.size ?: 0
        _uiState.update {
            it.copy(
                consoleLines = appendConsoleLine(
                    lines = it.consoleLines,
                    message = "simulated delivery failure: no ACK records=$count",
                ),
            )
        }
        _effects.tryEmit(
            OutboxLabEffect.ShowToast(
                message = "Failure simulated. No ACK was sent, so Read batch should return the same records again.",
            ),
        )
    }

    override fun onCleared() {
        doorbellJob?.cancel()
        runCatching {
            outbox.stop()
        }
        super.onCleared()
    }

    private fun runAction(
        name: String,
        nextStep: String? = null,
        action: suspend () -> String,
    ) {
        viewModelScope.launch {
            _uiState.update {
                it.copy(busyAction = name)
            }
            val result = runCatching {
                withContext(Dispatchers.IO) {
                    action()
                }
            }.getOrElse { throwable ->
                "${name} failed: ${throwable.javaClass.simpleName}"
            }
            val stats = readStats()
            _uiState.update {
                it.copy(
                    stats = stats,
                    busyAction = null,
                    consoleLines = appendConsoleLine(
                        lines = it.consoleLines,
                        message = result,
                    ),
                )
            }
            if (nextStep != null) {
                _effects.emit(
                    OutboxLabEffect.ShowToast(nextStep),
                )
            }
        }
    }

    private suspend fun readStats() = withContext(Dispatchers.IO) {
        outbox.getStats()
    }

    private fun requireStarted(action: String): Boolean {
        if (_uiState.value.stats.isStarted) {
            return true
        }
        val message = "$action blocked: start Runtime first"
        _uiState.update {
            it.copy(
                consoleLines = appendConsoleLine(
                    lines = it.consoleLines,
                    message = message,
                ),
            )
        }
        _effects.tryEmit(
            OutboxLabEffect.ShowToast(
                message = "Start Runtime first, then run $action.",
            ),
        )
        return false
    }

    private fun startDoorbellListener() {
        if (doorbellJob?.isActive == true) {
            return
        }
        doorbellJob = viewModelScope.launch {
            doorbells.events().collect(::handleDoorbell)
        }
    }

    private suspend fun handleDoorbell(event: OutboxDoorbellEvent) {
        _uiState.update {
            it.copy(
                lastDoorbellEvent = event,
                doorbellCount = it.doorbellCount + 1L,
                consoleLines = appendConsoleLine(
                    lines = it.consoleLines,
                    message = "doorbell event=$event",
                ),
            )
        }
        when (event) {
            OutboxDoorbellEvent.DATA_AVAILABLE -> {
                if (_uiState.value.currentBatch == null) {
                    readBatchInternal(
                        trigger = "doorbell",
                        showToast = true,
                    )
                } else {
                    _effects.emit(
                        OutboxLabEffect.ShowToast(
                            message = "Doorbell received. Finish the loaded batch with ACK before reading the next one.",
                        ),
                    )
                }
            }
            OutboxDoorbellEvent.DROPPED_RECORD -> {
                _effects.emit(
                    OutboxLabEffect.ShowToast(
                        message = "Native reported dropped records. Check pressure counters.",
                    ),
                )
            }
            OutboxDoorbellEvent.HANDSHAKE,
            OutboxDoorbellEvent.UNKNOWN,
            -> Unit
        }
    }

    private suspend fun readBatchInternal(
        trigger: String,
        showToast: Boolean,
    ) {
        val actionName = if (trigger == "manual") {
            "Manual read"
        } else {
            "Doorbell read"
        }
        _uiState.update {
            it.copy(busyAction = actionName)
        }
        val batch = withContext(Dispatchers.IO) {
            outbox.readNextBatch(
                maxRecords = LAB_MAX_BATCH_RECORDS,
                maxBytes = LAB_MAX_BATCH_BYTES,
            )
        }
        val stats = readStats()
        _uiState.update {
            it.copy(
                currentBatch = batch,
                stats = stats,
                busyAction = null,
                consoleLines = appendConsoleLine(
                    lines = it.consoleLines,
                    message = "read batch trigger=$trigger records=${batch?.records?.size ?: 0}",
                ),
            )
        }
        if (showToast) {
            _effects.emit(
                OutboxLabEffect.ShowToast(
                    message = if (batch?.records.isNullOrEmpty()) {
                        "No records returned. Write or burst first."
                    } else {
                        "Batch loaded by $trigger. ACK after delivery success, or simulate failure to keep it pending."
                    },
                ),
            )
        }
    }
}

private fun appendConsoleLine(
    lines: List<String>,
    message: String,
): List<String> {
    return buildList {
        add("${System.currentTimeMillis()}  $message")
        addAll(lines)
    }.take(MAX_CONSOLE_LINES)
}
