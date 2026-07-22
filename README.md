# AndroidOutBox

AndroidOutBox is a small Android native-backed outbox for app-owned logs and
events.

It is not a Logcat reader. It is not a crash reporter. It is not an
observability SDK. It is not tied to Sentry, Datadog, Firebase, Hilt,
WorkManager, or any backend.

## Core Responsibility

- accept structured or text records from app code
- store records in a bounded native/file-first outbox
- expose batches for the app to drain
- commit progress only after app ACK
- preserve pending records across process restart
- enforce queue and disk limits
- report pressure and drop stats

The app decides where, when, and how to sink the data.

## Kotlin API

```kotlin
val outbox = AndroidOutboxFactory.create()

outbox.start(
    OutboxConfig(
        spoolDirectoryPath = context.cacheDir.resolve("android-outbox").absolutePath,
    ),
)

outbox.write(
    level = OutboxRecordLevel.INFO,
    category = "sample.event",
    payload = """{"message":"hello"}""",
)

outbox.flush()

val batch = outbox.readNextBatch(
    providerId = OutboxConfig.DEFAULT_PROVIDER_ID,
    maxRecords = 32,
    maxBytes = 64 * 1024,
)

if (batch != null) {
    // Send records wherever the app wants. ACK only after delivery succeeds.
    outbox.ack(ackToken = batch.ackToken)
}
```

## Design Principles

- small API
- bounded resources
- file-first persistence
- ACK-based delivery
- thin JNI
- Kotlin controls, native stores
- no dependency on DI, app framework, or vendor SDK
- no hidden network
- no background work unless the app opts in

## Modules

- `:android-outbox`: the library module.
- `:app`: sample playground for manual Android experiments.

## Useful Commands

```bash
./gradlew :android-outbox:testNativeHost
./gradlew :android-outbox:testDebugUnitTest
./gradlew :android-outbox:assembleDebug
```

Native stress tests are opt-in:

```bash
./gradlew :android-outbox:testNativeHostStress -PandroidOutboxStress=true --console=plain
```
