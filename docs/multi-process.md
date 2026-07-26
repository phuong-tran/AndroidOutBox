# Multi-Process Skeleton

AndroidOutBox is process-local. In a multi-process Android app, do not let more
than one process open the same spool directory. Prefer one owner process that
hosts the outbox and exposes a small IPC facade to the other processes.

This page is a skeleton to copy and adapt. It is not a separate runtime module.

## Shape

```text
main process
  OutboxService
    AndroidOutboxFactory.create()
    app-private spool directory
    native doorbell reader

other app processes
  OutboxClient
    bind to OutboxService
    write/read/ack through Binder
    receive provider-neutral doorbell callbacks
```

The important rule is single ownership: only `OutboxService` touches the native
outbox and its spool directory. Other processes use IPC.

## Manifest

Keep the service app-private unless you intentionally design a cross-app API.

```xml
<service
    android:name=".outbox.OutboxService"
    android:exported="false" />
```

If you need the owner to run in a specific process, set that on the service and
make every other process bind to it instead of creating its own outbox instance.

## AIDL

Use your app package and parcelable types. Doorbells are provider-neutral wake-up
hints; the provider cursor is selected by `readNextBatch(providerId, ...)`.

```aidl
package com.example.app.outbox;

import com.example.app.outbox.IOutboxDoorbellCallback;
import com.example.app.outbox.OutboxBatchParcel;

interface IOutboxService {
    boolean write(int level, String category, String payload);
    boolean flush();
    boolean forceSync();
    OutboxBatchParcel readNextBatch(String providerId, int maxRecords, int maxBytes);
    boolean ack(String providerId, in byte[] ackToken);
    void registerDoorbellCallback(IOutboxDoorbellCallback callback);
    void unregisterDoorbellCallback(IOutboxDoorbellCallback callback);
}
```

Declare the parcelable for AIDL:

```aidl
package com.example.app.outbox;

parcelable OutboxBatchParcel;
```

```aidl
package com.example.app.outbox;

interface IOutboxDoorbellCallback {
    void onDoorbell(int doorbellType);
}
```

The batch can stay close to AndroidOutBox's public model:

```kotlin
@Parcelize
data class OutboxBatchParcel(
    val records: List<String>,
    val ackToken: ByteArray,
) : Parcelable
```

Records are raw spool lines. App code owns parsing, filtering, transport, and
privacy policy.

## Service Owner

Use `RemoteCallbackList` for Binder callback lifecycle. It automatically handles
dead remote binders better than a plain mutable map.

```kotlin
private fun Int.toOutboxRecordLevel(): OutboxRecordLevel {
    return when (this) {
        0 -> OutboxRecordLevel.TRACE
        1 -> OutboxRecordLevel.DEBUG
        2 -> OutboxRecordLevel.INFO
        3 -> OutboxRecordLevel.WARN
        4 -> OutboxRecordLevel.ERROR
        5 -> OutboxRecordLevel.FATAL
        else -> OutboxRecordLevel.INFO
    }
}

private fun OutboxDoorbellEvent.toIpcType(): Int {
    return when (this) {
        OutboxDoorbellEvent.HANDSHAKE -> 0
        OutboxDoorbellEvent.DATA_AVAILABLE -> 1
        OutboxDoorbellEvent.DROPPED_RECORD -> 2
        OutboxDoorbellEvent.UNKNOWN -> -1
    }
}

class OutboxService : Service() {
    private val callbacks = RemoteCallbackList<IOutboxDoorbellCallback>()
    private val serviceScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private lateinit var outbox: AndroidOutbox
    private lateinit var doorbells: BlockingOutboxDoorbellChannel

    private val binder = object : IOutboxService.Stub() {
        override fun write(level: Int, category: String, payload: String): Boolean {
            return outbox.write(
                level = level.toOutboxRecordLevel(),
                category = category,
                payload = payload,
            )
        }

        override fun flush(): Boolean = outbox.flush()

        override fun forceSync(): Boolean = outbox.forceSync()

        override fun readNextBatch(
            providerId: String,
            maxRecords: Int,
            maxBytes: Int,
        ): OutboxBatchParcel? {
            return outbox.readNextBatch(
                providerId = providerId,
                maxRecords = maxRecords,
                maxBytes = maxBytes,
            )?.let { batch ->
                OutboxBatchParcel(
                    records = batch.records,
                    ackToken = batch.ackToken,
                )
            }
        }

        override fun ack(providerId: String, ackToken: ByteArray): Boolean {
            return outbox.ack(providerId = providerId, ackToken = ackToken)
        }

        override fun registerDoorbellCallback(callback: IOutboxDoorbellCallback) {
            callbacks.register(callback)
        }

        override fun unregisterDoorbellCallback(callback: IOutboxDoorbellCallback) {
            callbacks.unregister(callback)
        }
    }

    override fun onCreate() {
        super.onCreate()
        outbox = AndroidOutboxFactory.create()
        outbox.start(
            OutboxConfig(
                spoolDirectoryPath = noBackupFilesDir
                    .resolve("android-outbox")
                    .absolutePath,
            ),
        )
        doorbells = BlockingOutboxDoorbellChannel(outbox)
        serviceScope.launch {
            doorbells.events().collect { event ->
                notifyDoorbell(event)
            }
        }
    }

    override fun onBind(intent: Intent?): IBinder = binder

    override fun onDestroy() {
        callbacks.kill()
        serviceScope.cancel()
        outbox.stop()
        super.onDestroy()
    }

    private fun notifyDoorbell(event: OutboxDoorbellEvent) {
        val count = callbacks.beginBroadcast()
        try {
            repeat(count) { index ->
                runCatching {
                    callbacks.getBroadcastItem(index).onDoorbell(event.toIpcType())
                }
            }
        } finally {
            callbacks.finishBroadcast()
        }
    }
}
```

## Client Proxy

Secondary processes bind to the service and use a proxy. Keep reconnect,
permission, and lifecycle policy in app code.

```kotlin
private fun OutboxRecordLevel.toIpcLevel(): Int {
    return when (this) {
        OutboxRecordLevel.TRACE -> 0
        OutboxRecordLevel.DEBUG -> 1
        OutboxRecordLevel.INFO -> 2
        OutboxRecordLevel.WARN -> 3
        OutboxRecordLevel.ERROR -> 4
        OutboxRecordLevel.FATAL -> 5
    }
}

private fun Int.toDoorbellEvent(): OutboxDoorbellEvent {
    return OutboxDoorbellEvent.fromNativeValue(this)
}

class IpcOutboxClient(
    private val service: () -> IOutboxService?,
) {
    fun write(
        level: OutboxRecordLevel,
        category: String,
        payload: String,
    ): Boolean {
        return service()?.write(level.toIpcLevel(), category, payload) ?: false
    }

    fun readNextBatch(
        providerId: String,
        maxRecords: Int,
        maxBytes: Int,
    ): OutboxBatch? {
        return service()
            ?.readNextBatch(providerId, maxRecords, maxBytes)
            ?.let { parcel ->
                OutboxBatch(
                    records = parcel.records,
                    ackToken = parcel.ackToken,
                )
            }
    }

    fun ack(providerId: String, ackToken: ByteArray): Boolean {
        return service()?.ack(providerId, ackToken) ?: false
    }

    fun flush(): Boolean = service()?.flush() ?: false

    fun forceSync(): Boolean = service()?.forceSync() ?: false
}
```

If you want to reuse `AndroidOutboxSinkRunner` in a secondary process, make a
small proxy that implements `AndroidOutbox` by delegating to this client, then
feed it doorbells from `IOutboxDoorbellCallback`.

```kotlin
val doorbells = Channel<OutboxDoorbellEvent>(capacity = Channel.CONFLATED)

val callback = object : IOutboxDoorbellCallback.Stub() {
    override fun onDoorbell(doorbellType: Int) {
        doorbells.trySend(doorbellType.toDoorbellEvent())
    }
}
```

Doorbells are wake-up hints, not durable work items. If the secondary process
misses a callback, it can still call `readNextBatch()` later because the spool
and provider cursor live in the owner process.

## Notes

- Bind to the owner service before writing from a secondary process.
- Keep `write()` payloads compact; Binder also has transaction size limits.
- Use one runner per provider id.
- Keep ACK after successful delivery only.
- `flush()` drains accepted records to the spool writer. `forceSync()` is the
  optional OS-level storage barrier.
- Do not create an AndroidOutBox instance in each process against the same
  directory.
