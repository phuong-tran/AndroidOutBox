# AndroidOutBox Technical Notes

## Table of Contents

- [Why It Exists](#why-it-exists)
- [Scope Boundary](#scope-boundary)
- [When To Use It](#when-to-use-it)
- [When Not To Use It](#when-not-to-use-it)
- [Core Model](#core-model)
- [Runtime Flow](#runtime-flow)
- [Delivery Guarantees](#delivery-guarantees)
- [How Outbox Files Work](#how-outbox-files-work)
- [Configuration Semantics](#configuration-semantics)
- [Storage Directory](#storage-directory)
- [Process Ownership](#process-ownership)
- [Network Sinks](#network-sinks)
- [Sink Orchestration](#sink-orchestration)
- [Can It Log Crashes?](#can-it-log-crashes)
- [Crash Logging Pattern](#crash-logging-pattern)
- [Payload Guidance](#payload-guidance)
- [Operational Notes](#operational-notes)

## Why It Exists

AndroidOutBox exists for apps that want app-owned event durability without
adopting a full observability SDK.

It is a bounded best-effort local outbox, not a database transactional outbox.
It does not provide an atomic boundary between updating application data and
publishing an event. Its job is narrower: accept app-owned records, keep recent
records locally when resources permit, and let the app drain them later.

Many SDKs try to solve several problems at once: crash reporting, tracing,
network instrumentation, breadcrumbs, background delivery, device contexts, and
remote transport. That can be useful for some teams, but it also means the
SDK owns a large part of the runtime behavior.

AndroidOutBox takes a narrower position:

- the app owns what should be logged
- the app owns where records are delivered
- the app owns retry, backoff, privacy, sampling, and payload shape
- the native layer only provides a bounded file-first outbox

The goal is not to replace every observability tool. The goal is to provide a
small durable handoff point between app code and any later consumer.

AndroidOutBox protects the application first. Records may be dropped under
memory pressure, disk limits, retention limits, storage cleanup, corruption, or
other runtime constraints. Application behavior must never depend on an outbox
write succeeding.

## Scope Boundary

AndroidOutBox is intentionally strict about scope. It does not try to observe
the whole application, infer user intent, or become a second runtime inside the
app.

Many observability SDKs provide value by automatically collecting a wide range
of signals: exceptions, breadcrumbs, lifecycle events, network tracing,
performance spans, ANR or crash data, device context, user actions, and
background delivery. That can be convenient when an app wants a plug-and-play
observability product, but every enabled SDK also gets closer to core runtime
ownership.

The cost becomes more visible when an app combines several large SDKs. Multiple
SDKs may instrument similar layers, collect overlapping context, compete for
startup time, allocate on hot paths, register lifecycle hooks, schedule
background work, or add vendor-specific payload assumptions. The result can be
more coupling, more runtime behavior to reason about, and a higher chance that
telemetry work affects the user experience it is supposed to measure.

AndroidOutBox chooses a smaller contract:

```text
app detects a meaningful event
app chooses the context worth keeping
write best-effort record to AndroidOutBox
app-owned sink drains records in batches
ACK only after the sink succeeds
```

The caller must explicitly emit the record. AndroidOutBox does not capture
network calls, exceptions, lifecycle changes, user actions, or device state on
its own. That is deliberate. The domain layer usually knows which failures are
meaningful; the outbox only provides a bounded place to hold those records until
an app-owned consumer drains them.

This library is not extreme about reliability. It is extreme about scope. It
does not try to keep everything, capture everything, or send everything at any
cost. It protects the app first and leaves product semantics, privacy,
sampling, retry policy, and backend choice to the application.

## When To Use It

Use AndroidOutBox when you need a lightweight local outbox for records such as:

- handled business failures
- API failure summaries
- important state transition events
- delivery retry experiments
- crash or runtime summaries emitted by an app-owned monitor
- vendor-neutral telemetry that may later go to different backends

It is especially useful when the call site must stay cheap. The app writes a
compact record, native persists it, and Kotlin drains it later through a
doorbell/read/ACK flow.

Good fit:

```text
app code -> write compact record -> native file-first outbox
native doorbell -> Kotlin reads batch -> app transporter sends it
success -> ACK
failure -> no ACK, retry later
```

## When Not To Use It

Do not use AndroidOutBox as:

- a Logcat reader
- a general analytics SDK
- a crash reporter replacement
- a long-term audit database
- a hidden network uploader
- a background worker framework
- an unbounded event store
- a place to dump raw request/response bodies

AndroidOutBox is intentionally best-effort and bounded. When disk or queue
limits are reached, old data may be dropped according to the configured limits.
That is a feature, not a bug: the library should protect the app first.

## Core Model

AndroidOutBox has four important concepts.

### Record

A record is a compact payload written by app code:

```kotlin
outbox.write(
    level = OutboxRecordLevel.ERROR,
    category = "demo.document.sync",
    payload = """{"message":"SYNC001 Document sync failed"}""",
)
```

The payload is app-owned. Native does not parse JSON and does not know about
Sentry, Loki, Datadog, Firebase, or any other backend.

### Doorbell

A doorbell is a native-to-Kotlin wake-up signal. It does not contain records.
It only tells Kotlin that something happened, such as durable data becoming
available.

```text
doorbell = wake up
readNextBatch = pull durable records
```

This keeps the signal path small and avoids moving large payloads through the
wake-up channel.

### Batch

A batch is a peek into durable records. Reading a batch does not commit
delivery progress.

```kotlin
val batch = outbox.readNextBatch(
    maxRecords = 32,
    maxBytes = 64 * 1024,
)
```

The app can inspect, transform, or deliver the records. If delivery fails, the
app should not ACK.

### ACK

ACK is the commit point. Call ACK only after the consumer accepts the loaded
batch.

```kotlin
if (uploadSucceeded) {
    outbox.ack(ackToken = batch.ackToken)
}
```

The token is native-owned and opaque. Kotlin should pass it back as-is.

## Runtime Flow

The expected runtime flow is doorbell-first:

1. Start AndroidOutBox with an app-private spool directory.
2. Drain once immediately after startup to pick up records left from a previous
   process lifetime.
3. App code writes compact records.
4. Native persists records to the bounded spool.
5. Native rings a doorbell such as `DATA_AVAILABLE`.
6. Kotlin wakes up and reads a batch.
7. The app-owned transporter sends the batch wherever it wants.
8. On success, Kotlin ACKs the batch.
9. On failure, Kotlin does not ACK; the same records remain pending while they
   are still retained.

The library does not start a background service or schedule WorkManager by
itself. If an app wants background draining, the app should wire that policy
above AndroidOutBox.

Doorbells are wake-up hints, not the source of truth. They may be coalesced or
missed across lifecycle boundaries. The durable spool is authoritative, so a
consumer should always perform an initial drain after `start()`, then use
doorbells to wake future drains.

## Delivery Guarantees

AndroidOutBox provides retryable ACK-based delivery while records remain
retained. It does not provide exactly-once delivery or indefinite retention.

| Situation | Result |
|---|---|
| `write()` cannot hand the record to native | The call returns `false`; the app flow should continue. |
| Native queue is full | The record may be dropped and pressure counters are updated. |
| A record is accepted but the process dies before the writer drains it | The record may be lost. |
| A record is written to the spool and not ACKed | A later read can return it again while it is still retained. |
| Network send succeeds but the app dies before ACK | The same batch may be delivered again. Consumers should tolerate duplicates. |
| ACK succeeds | The provider cursor commits past the loaded batch. |
| A sink fails to send | Do not ACK; retry later while the record remains retained. |
| Retention deletes old segments before a provider ACKs them | Those old records are lost for that provider. |
| A doorbell is missed | Initial/manual drain can still find retained records. |

ACK is the commit signal. Reading a batch is a peek; it does not remove or
commit the batch.

Provider cursors isolate delivery progress, not retention. Multiple provider
ids can advance independently, but all providers still share the same bounded
spool budget. A slow provider can lose old unacknowledged records when retention
removes old segments to protect application resources.

## How Outbox Files Work

AndroidOutBox stores accepted records in native-managed spool files. These files
are not plain append-only logs for humans to tail. They are the durable side of a
bounded outbox, so the important behavior is rotation, cursor movement, and ACK.

The write path is intentionally simple:

1. App code calls `write()` with a compact record.
2. Native accepts the record into a bounded queue when capacity allows it.
3. A writer thread drains the queue into the active spool segment.
4. Native rings a doorbell when durable records may be available.

Spool files are split into numbered segments. The active segment is appended to
until it reaches the configured segment size limit, then AndroidOutBox rotates
to a new segment. Old segments are retained only within the configured storage
budget. This keeps disk use bounded and prevents telemetry from becoming more
important than the application.

Each provider has its own cursor. A provider is just a consumer id chosen by the
app, such as `default`, `primary`, `secondary`, or `debug-upload`. Native does
not know what those providers mean. It only tracks where each provider has
successfully committed delivery progress.

`readNextBatch()` is a peek operation. It reads from the provider cursor and
returns records plus an opaque ACK token, but it does not remove records and it
does not advance the durable cursor.

```text
readNextBatch(providerId) = show this provider the next retained records
```

`ack()` is the commit operation. The app should call it only after the provider
has successfully handled the batch, for example after a network sink returns
success.

```text
ack(providerId, ackToken) = this provider completed delivery up to this point
```

If a provider fails to upload, the app should not ACK. A later retry can read the
same batch again while the records are still retained. If the app process is
killed before ACK, the provider cursor remains behind, so startup drain can
resume from the last committed cursor.

This is still bounded best-effort delivery. ACK controls consumer progress, not
infinite retention. If old segments must be deleted because of retention limits,
disk pressure, cleanup, or corruption handling, unacknowledged records in those
segments are lost for any provider that has not advanced past them.

## Configuration Semantics

AndroidOutBox configuration values are protection boundaries, not delivery
promises. They define how much native memory and disk space the outbox is
allowed to use before it starts rejecting or deleting records to protect the
application.

| Parameter | Default | Purpose | When the limit is reached |
|---|---|---|---|
| `spoolDirectoryPath` | Required | App-private directory for spool segments and provider cursors. | If the directory cannot be created or opened, `start()` fails and the outbox stays stopped. |
| `defaultProviderId` | `default` | Cursor id used when a caller does not pass an explicit provider id. | It must match the provider id format. Invalid ids are rejected by `OutboxConfig`. |
| `queueCapacity` | `256` | Maximum number of records that may wait in native memory before the writer persists them. | When the queue is full, new writes are rejected, `write()` returns `false`, and pressure stats are updated. |
| `maxRecordBytes` | `4096` | Maximum payload size for one record. | Oversized records are rejected before enqueueing. AndroidOutBox does not split or partially write a record. |
| `maxSegmentSizeBytes` | `524288` | Approximate size at which the active spool segment rotates. | The writer rolls to a new segment instead of growing the active segment indefinitely. |
| `maxArchivedSegments` | `3` | Number of rolled segments retained beside the active segment. | Oldest archived segments are deleted when the retained segment count exceeds the configured budget. |

The approximate disk budget is:

```text
maxSegmentSizeBytes * (maxArchivedSegments + 1)
```

The `+ 1` is the active segment. For the defaults, AndroidOutBox keeps roughly
`512KB * 4`, or about `2MB`, before normal segment cleanup can delete old
segments.

`queueCapacity` protects the hot path and native memory. A larger queue can
absorb longer bursts, but it reserves more native memory because payload slots
are allocated according to `maxRecordBytes`.

`maxRecordBytes` protects the outbox from becoming a raw payload dump. Keep
records compact and sanitized. Prefer summaries, ids, error codes, paths, and
small metadata over raw request or response bodies.

`maxSegmentSizeBytes` and `maxArchivedSegments` protect disk usage. Rotation
creates new segments; retention cleanup removes old ones. Reading a batch does
not delete segment files. ACK only moves a provider cursor. Segment deletion is
driven by bounded retention, not by a read call.

If a provider cursor points into a segment that has already been removed, old
unacknowledged records from that segment are no longer available to that
provider. The provider should continue from the next retained records. This is
intentional: a slow or broken sink must not force the app to keep growing disk
usage forever.

## Storage Directory

Use an app-private directory for `OutboxConfig.spoolDirectoryPath`.

Recommended defaults:

- `context.noBackupFilesDir.resolve("android-outbox")` for local records that
  should survive normal cache cleanup but should not be restored to a new
  device by Android Auto Backup.
- `context.filesDir.resolve("android-outbox")` for app-private records that
  should survive normal cache cleanup.
- `context.cacheDir.resolve("android-outbox")` only when records may be
  discarded by the operating system.

`cacheDir` is valid for disposable telemetry, but the OS may clear it. Do not
interpret process-restart survival as a promise that records survive all storage
cleanup.

## Process Ownership

One spool directory should be owned by one Android process.

AndroidOutBox does not currently define inter-process locking semantics for two
processes writing, rotating, reading, ACKing, and cleaning the same spool
directory. Multi-process apps should either:

- start AndroidOutBox in only one process, or
- use a separate spool directory per process, for example
  `android-outbox/main` and `android-outbox/remote`.

## Network Sinks

AndroidOutBox does not upload records. Network delivery should live in an
app-owned sink layer above the outbox.

Common sink examples:

- Sentry envelope/event endpoint
- Loki push endpoint
- Datadog intake
- OpenTelemetry collector
- an internal telemetry gateway
- a debug-only local HTTP collector

The core rule is simple:

```text
AndroidOutBox stores records
SinkTransporter sends records
ACK happens only after that sink succeeds
```

One app can keep the sink layer boring and explicit. Define a small sink
interface for the transport policy:

```kotlin
interface OutboxSink {
    val providerId: String

    suspend fun send(records: List<String>): Boolean
}

class SentryOutboxSink(
    private val endpoint: String,
    private val httpClient: HttpClient,
) : OutboxSink {
    override val providerId: String = "sentry"

    override suspend fun send(records: List<String>): Boolean {
        val body = buildSentryPayload(records)
        return httpClient.post(endpoint, body).isSuccessful
    }
}

class LokiOutboxSink(
    private val endpoint: String,
    private val httpClient: HttpClient,
) : OutboxSink {
    override val providerId: String = "loki"

    override suspend fun send(records: List<String>): Boolean {
        val body = buildLokiPayload(records)
        return httpClient.post(endpoint, body).isSuccessful
    }
}
```

For production code, prefer adapting that sink through `AndroidOutboxSinkRunner`
instead of letting multiple call sites read and ACK batches directly:

```kotlin
class ProviderOutboxRunner(
    outbox: AndroidOutbox,
    private val sink: OutboxSink,
) : AndroidOutboxSinkRunner(
    outbox = outbox,
    providerId = sink.providerId,
) {
    override suspend fun send(records: List<String>): Boolean {
        return sink.send(records)
    }
}
```

In a real app, the drain runner should usually be driven by a coroutine-facing
doorbell `Flow`. AndroidOutBox ships a default blocking channel, so apps do not
need to copy fd or native-read details:

```kotlin
val doorbells = BlockingOutboxDoorbellChannel(outbox)
```

The runner owns the read/send/ACK sequence for one provider cursor:

```kotlin
val sink = SentryOutboxSink(
    endpoint = sentryEndpoint,
    httpClient = httpClient,
)
val runner = ProviderOutboxRunner(
    outbox = outbox,
    sink = sink,
)

scope.launch {
    runner.run(doorbells)
}
```

The important chain inside the runner is:

```text
initial drain -> doorbell -> readNextBatch -> post to network sink -> ack()
```

If `post` fails, stop the drain loop for that sink and do not ACK. Retry policy
belongs above AndroidOutBox.

If Sentry succeeds but Loki fails, only ACK Sentry:

```text
sentry send success -> ack providerId="sentry"
loki send failure -> no ack providerId="loki"
```

This is why provider ids are opaque. Native does not know what Sentry, Loki, or
Telemetry means. It only keeps independent cursors. The app decides how many
sinks exist, what payload format each sink needs, and when each provider is
allowed to advance.

For simple apps, use one provider id and one sink. For apps that need multiple
backends, prefer separate provider ids so one failing backend does not block the
cursor of another backend.

## Sink Orchestration

AndroidOutBox expects the application to centralize drain ownership. Do not let
many independent workers call `readNextBatch()` for the same provider id.

The easiest way is to extend `AndroidOutboxSinkRunner`; it serializes drain
calls for one provider cursor and ACKs only after `send()` returns true.

The correct shape is one orchestrator or runner owning each provider cursor:

```text
many app writers
-> AndroidOutBox.write(...)
-> native/file outbox
-> doorbell hint
-> one provider drain owner
-> readNextBatch()
-> post to sink
-> ack() only after success
```

The write side may have many producers. Different threads, dispatchers, screens,
or use cases can write records concurrently. That is a producer-side concern.
The drain side is different: a provider cursor is ordered state, so reads and
ACKs for that provider should be serialized by one owner.

A doorbell is not a durable queue of work items. It is only a wake-up hint that
records may be available. Multiple doorbells may arrive while a drain is already
posting a batch to the network. That should not create multiple drain jobs for
the same provider. The durable source of truth is still the native/file outbox,
and the cursor only advances after ACK.

Prefer this model:

```text
doorbell received while idle -> start drain
doorbell received while draining -> mark dirty or ignore
drain reads batch -> sink sends batch -> ACK on success
sink fails -> stop draining and do not ACK
later trigger -> retry from the same provider cursor
```

Avoid this model:

```text
doorbell 1 -> launch drain job A
doorbell 2 -> launch drain job B for the same provider
doorbell 3 -> launch drain job C for the same provider
```

Concurrent drains for the same provider can produce duplicate sends, unordered
ACKs, cursor races, or retry behavior that is hard to reason about. If the app
needs more throughput, increase batch size, tune `maxBytes`, use separate
provider ids for truly independent sinks, or optimize the sink transport. Do not
parallelize reads from the same provider cursor.

The app may still use coroutines, `Flow`, a channel, WorkManager, connectivity
callbacks, or app-start hooks to trigger the orchestrator. Those mechanisms are
just triggers. The invariant remains the same: one active drain path per
provider cursor.

## Can It Log Crashes?

Yes, but with an important boundary:

AndroidOutBox can persist a compact crash summary if the app already owns a
crash monitor or uncaught exception handler.

AndroidOutBox is not a crash reporter. It does not:

- install a crash handler automatically
- capture ANRs automatically
- symbolicate stack traces
- upload ProGuard/R8 mapping files
- replace Crashlytics, Sentry Crash Reporting, or platform crash tools
- perform network delivery from the crashing process

This distinction matters. Crash handling is a sensitive runtime path. A crash
handler should do the minimum amount of work required, then delegate to the
previous/default handler so the platform and existing crash reporter can finish
their normal work.

## Crash Logging Pattern

An app-owned crash monitor can write a small `FATAL` record and flush it. The
record should be a summary, not a full crash report.

Recommended crash payload fields:

- signal name, for example `runtime_crash`
- source, for example `uncaught_exception`
- fatal flag
- thread name
- exception class
- short message
- message hash
- stack hash
- top frame, trimmed
- timestamp

Example shape:

```text
signal=runtime_crash
source=uncaught_exception
fatal=true
thread=main
exception=java.lang.ArithmeticException
message=divide_by_zero
message_hash=a47e902a
stack_hash=79ae2623
top_frame=com.example.app.EditorViewModel.save(EditorViewModel.kt:42)
timestamp_ms=1784574973006
```

Example monitor sketch:

```kotlin
class AndroidOutboxCrashMonitor(
    private val outbox: AndroidOutbox,
) : Thread.UncaughtExceptionHandler {

    private var previousHandler: Thread.UncaughtExceptionHandler? = null

    fun startMonitor() {
        previousHandler = Thread.getDefaultUncaughtExceptionHandler()
        Thread.setDefaultUncaughtExceptionHandler(this)
    }

    override fun uncaughtException(
        thread: Thread,
        throwable: Throwable,
    ) {
        persistFatalCrash(thread, throwable)
        previousHandler?.uncaughtException(thread, throwable) ?: kotlin.system.exitProcess(2)
    }

    private fun persistFatalCrash(
        thread: Thread,
        throwable: Throwable,
    ) {
        runCatching {
            val accepted = outbox.write(
                level = OutboxRecordLevel.FATAL,
                category = "runtime.crash",
                payload = throwable.toCompactCrashPayload(thread),
            )
            if (accepted) {
                outbox.flush()
            }
        }
    }

    private companion object {
        private const val MAX_MESSAGE_LENGTH = 160
        private const val MAX_TOP_FRAME_LENGTH = 220
        private val WHITESPACE_REGEX = Regex("\\s+")

        private fun Throwable.toCompactCrashPayload(thread: Thread): String {
            val stackTraceText = stackTraceToString()
            val topFrame = stackTrace.firstOrNull()?.toString().orEmpty()
            return buildString {
                append("signal=runtime_crash")
                append(" source=uncaught_exception")
                append(" fatal=true")
                append(" thread=").append(thread.name.toPayloadValue())
                append(" exception=").append(javaClass.name.toPayloadValue())
                append(" message=").append(message.orEmpty().take(MAX_MESSAGE_LENGTH).toPayloadValue())
                append(" message_hash=").append(message.orEmpty().stableHash())
                append(" stack_hash=").append(stackTraceText.stableHash())
                append(" top_frame=").append(topFrame.take(MAX_TOP_FRAME_LENGTH).toPayloadValue())
                append(" timestamp_ms=").append(System.currentTimeMillis())
            }
        }

        private fun String.toPayloadValue(): String {
            return ifBlank { "none" }
                .replace(WHITESPACE_REGEX, "_")
                .replace('|', '_')
                .replace('\t', '_')
                .replace('\n', '_')
                .replace('\r', '_')
        }

        private fun String.stableHash(): String {
            return Integer.toHexString(hashCode())
        }
    }
}
```

Rules for crash paths:

- keep the payload small
- avoid remote network calls
- avoid expensive formatting
- avoid raw PII, tokens, or request bodies
- prefer hashes over full stack dumps
- flush only the local outbox
- let the default crash handler continue

The next healthy app launch can drain the crash summary from AndroidOutBox and
send it to any app-owned backend.

## Payload Guidance

Payloads should be compact, sanitized, and app-owned.

Good payloads are:

- single-line
- bounded in size
- free of secrets and PII
- meaningful at the business or app boundary
- portable across backends
- understandable without lower-layer implementation details

Avoid payloads that force the emitting layer to know too much:

- raw HTTP headers
- full response bodies
- access tokens
- cookies
- device identifiers without consent
- full stack traces for ordinary business errors
- fields that require extra API calls just to log

The app should attach values only when the emitting flow already owns them
naturally.

## Operational Notes

AndroidOutBox is intentionally bounded:

- `queueCapacity` limits in-memory pressure
- `maxRecordBytes` rejects oversized records
- `maxSegmentSizeBytes` rolls spool segments
- `maxArchivedSegments` caps retained files

The outbox is best-effort. It is designed to preserve useful recent records
without risking app performance or unbounded disk growth.

For delivery, prefer this policy:

```text
read batch
try upload/send
if success -> ACK
if failure -> no ACK
retry later with backoff owned by the app
```

Multiple providers can use different provider ids if the app wants separate
read/ACK progress. Native still treats provider ids as opaque cursor names; it
does not know which backend a provider represents.
