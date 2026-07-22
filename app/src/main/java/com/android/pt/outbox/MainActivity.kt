package com.android.pt.outbox

import android.os.Bundle
import android.widget.Toast
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.activity.viewModels
import androidx.lifecycle.Lifecycle
import androidx.compose.runtime.getValue
import androidx.lifecycle.lifecycleScope
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import androidx.lifecycle.repeatOnLifecycle
import com.android.pt.outbox.ui.OutboxLabScreen
import com.android.pt.outbox.ui.theme.AndroidOutBoxTheme
import kotlinx.coroutines.launch

class MainActivity : ComponentActivity() {

    private val viewModel: OutboxLabViewModel by viewModels()

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()
        observeEffects()
        setContent {
            AndroidOutBoxTheme(
                darkTheme = true,
                dynamicColor = false,
            ) {
                val state by viewModel.uiState.collectAsStateWithLifecycle()
                OutboxLabScreen(
                    state = state,
                    onCategorySelected = viewModel::selectCategory,
                    onWriterSelected = viewModel::selectWriter,
                    onStart = viewModel::startOutbox,
                    onWriteOne = viewModel::writeOne,
                    onBurst = viewModel::burst,
                    onFlush = viewModel::flush,
                    onRefreshStats = viewModel::refreshStats,
                    onReadBatch = viewModel::readBatch,
                    onAck = viewModel::ackBatch,
                    onSimulateFailure = viewModel::simulateFailure,
                    onClearConsole = viewModel::clearConsole,
                )
            }
        }
    }

    private fun observeEffects() {
        lifecycleScope.launch {
            repeatOnLifecycle(Lifecycle.State.STARTED) {
                viewModel.effects.collect { effect ->
                    when (effect) {
                        is OutboxLabEffect.ShowToast -> {
                            Toast.makeText(
                                this@MainActivity,
                                effect.message,
                                Toast.LENGTH_LONG,
                            ).show()
                        }
                    }
                }
            }
        }
    }
}
