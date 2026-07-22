package io.github.phuongtran.androidoutbox

object NativeAndroidOutbox : AndroidOutbox {
    override fun start(config: OutboxConfig): Boolean {
        return synchronized(lifecycleLock) {
            if (!isAvailable || runtimeState is RuntimeState.Started) {
                return@synchronized false
            }

            val transport = runCatching {
                OutboxNativePipeTransport.adopt(nativeOpenPipes() ?: return@synchronized false)
            }.getOrNull() ?: return@synchronized false
            val client = OutboxNativeControlClient(transport)
            if (client.configure(config)) {
                runtimeState = RuntimeState.Started(client)
                true
            } else {
                client.closeNativePipes()
                client.close()
                false
            }
        }
    }

    override fun write(
        level: OutboxRecordLevel,
        category: String,
        payload: String,
    ): Boolean {
        return withStartedClient(default = false) { client ->
            client.write(
                level = level,
                category = category,
                payload = payload,
            )
        }
    }

    override fun flush(): Boolean {
        return withStartedClient(default = false) { client ->
            client.flush()
        }
    }

    override fun stop() {
        val client = synchronized(lifecycleLock) {
            when (val state = runtimeState) {
                RuntimeState.Stopped -> return
                is RuntimeState.Started -> {
                    runtimeState = RuntimeState.Stopped
                    state.client
                }
            }
        }
        client.stopNativeLogger()
        client.closeNativePipes()
        client.close()
    }

    override fun getStats(): OutboxStats {
        return withStartedClient(default = OutboxStats()) { client ->
            client.getStats()
        }
    }

    override fun readNextDoorbell(): OutboxDoorbellEvent? {
        return withStartedClient(default = null) { client ->
            client.readNextDoorbell()
        }
    }

    override fun readNextBatch(
        providerId: String,
        maxRecords: Int,
        maxBytes: Int,
    ): OutboxBatch? {
        return withStartedClient(default = null) { client ->
            client.readNextBatch(
                providerId = providerId,
                maxRecords = maxRecords,
                maxBytes = maxBytes,
            )
        }
    }

    override fun ack(
        providerId: String,
        ackToken: ByteArray,
    ): Boolean {
        return withStartedClient(default = false) { client ->
            client.ack(
                providerId = providerId,
                ackToken = ackToken,
            )
        }
    }

    internal val isAvailable: Boolean
        get() = isNativeLibraryLoaded

    private external fun nativeOpenPipes(): IntArray?

    private inline fun <T> withStartedClient(
        default: T,
        block: (OutboxNativeControlClient) -> T,
    ): T {
        return when (val state = runtimeState) {
            RuntimeState.Stopped -> default
            is RuntimeState.Started -> block(state.client)
        }
    }

    /**
     * Keep native loading lazy and isolated at the boundary so callers can fall
     * back to no-op when the library is unavailable.
     */
    private val isNativeLibraryLoaded: Boolean by lazy {
        runCatching {
            System.loadLibrary("android_outbox")
        }.isSuccess
    }

    private sealed interface RuntimeState {
        data object Stopped : RuntimeState

        data class Started(
            val client: OutboxNativeControlClient,
        ) : RuntimeState
    }

    private val lifecycleLock = Any()

    @Volatile
    private var runtimeState: RuntimeState = RuntimeState.Stopped
}
