# AndroidOutBox

[![CI](https://github.com/phuong-tran/AndroidOutBox/actions/workflows/ci.yml/badge.svg)](https://github.com/phuong-tran/AndroidOutBox/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Version](https://img.shields.io/badge/version-1.3.9-blue.svg)](#installation)

AndroidOutBox is a small Android native-backed outbox for app-owned logs and
events.

It is not a Logcat reader. It is not a crash reporter. It is not an
observability SDK. It is not tied to Sentry, Datadog, Firebase, WorkManager, or
any backend.

AndroidOutBox protects the application first. Like any bounded mobile logging
pipeline, it cannot guarantee that every accepted record survives power loss,
process death, OS cleanup, corruption, retention limits, or storage pressure.
Its contract is to make those trade-offs explicit while keeping app behavior
independent from logging success.

The app must explicitly decide what is worth recording. AndroidOutBox does not
auto-capture exceptions, lifecycle events, network calls, breadcrumbs, user
actions, or device context. It provides a bounded local handoff point; the app
keeps ownership of meaning, privacy, retry policy, and network delivery.

## Core Responsibility

- accept structured or text records from app code
- store records in a bounded native/file-first outbox
- expose batches for the app to drain
- commit progress only after app ACK
- attempt to preserve recent pending records across ordinary process restart
- enforce queue and disk limits
- report pressure and drop stats

The app decides where, when, and how to sink the data.

## Project Status

AndroidOutBox keeps a small public API with a documented and tested runtime
contract. Native/runtime internals may still evolve while preserving that
contract.

The project favors app safety over telemetry completeness. It is designed as a
bounded outbox, not an unbounded audit log.

## Delivery Model

AndroidOutBox is a bounded, best-effort local outbox designed to protect the
application first.

No bounded mobile logging SDK can promise lossless delivery in every runtime
failure window. Records can be lost around power-off, process death during a
write, storage cleanup, corruption, retention limits, or quota pressure. In
addition, AndroidOutBox may intentionally drop records when limits are reached
so logging cannot destabilize the host app. Writes avoid disk I/O on the caller
hot path and should fail open instead of destabilizing the application.

Reading a batch does not remove it. A provider cursor advances only after ACK,
allowing failed deliveries to be retried while the records are still retained.

ACK-based delivery does not guarantee that records are stored indefinitely.
Retention remains bounded, and old unacknowledged records may be discarded to
protect application resources.

## Distribution Model

This repository contains both the source code and the static Maven repository
used by consumers. The published artifacts live under the checked-in `maven/`
directory, following the normal Maven repository layout.

There is no separate SDK backend, package registry, or hidden download step. A
consumer points Gradle at the raw GitHub `maven/` directory and depends on the
AAR by Maven coordinate.

## Installation

Add the static Maven repository in `settings.gradle.kts` or the application
Gradle configuration:

```kotlin
dependencyResolutionManagement {
    repositories {
        google()
        mavenCentral()
        maven {
            url = uri("https://raw.githubusercontent.com/phuong-tran/AndroidOutBox/main/maven")
        }
    }
}
```

Then add the dependency:

```kotlin
implementation("io.github.phuongtran:android-outbox:1.3.9")
```

The AAR includes the Kotlin API and the native `libandroid_outbox.so` binaries
for Android ABIs. Gradle metadata, POM metadata, source jar, and checksums are
also committed under `maven/`.

The synchronous core API can be called from Kotlin or Java. The default
doorbell channel and `AndroidOutboxSinkRunner` use Kotlin coroutines, so apps
that use those helpers should provide their own coroutine scope.

## Kotlin API

```kotlin
val outbox = AndroidOutboxFactory.create()

outbox.start(
    OutboxConfig(
        spoolDirectoryPath = context.noBackupFilesDir
            .resolve("android-outbox")
            .absolutePath,
    ),
)

outbox.write(
    level = OutboxRecordLevel.ERROR,
    category = "checkout.failure",
    payload = """{"errorCode":"CHK_400","httpStatus":400}""",
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

`flush()` drains accepted records to the spool writer. Apps that intentionally
want an OS-level stable-storage barrier can call `forceSync()` from lifecycle or
memory-pressure hooks.

`write()` returns whether Kotlin handed a complete command frame to native. It
does not wait for native queue acceptance or disk persistence; watch
`getStats()` and doorbells for queue pressure and drops.

Payloads whose UTF-8 size reaches `maxRecordBytes` are rejected on the Kotlin
side before a command frame is allocated or written to the pipe. Categories are
bounded to `OutboxConfig.MAX_CATEGORY_BYTES` UTF-8 bytes.

The `64 * 1024` value above is only a default batch pull budget. Pipe frames are
length-prefixed and read to completion; oversized records are rejected at the
write/config boundary instead of by a 64 KiB pipe ceiling.

For production sinks, centralize read/send/ACK ownership with
`AndroidOutboxSinkRunner`:

```kotlin
val runner = AndroidOutboxSinkRunner(
    outbox = outbox,
    providerId = "primary",
) { records ->
    transport.post(records)
}

val doorbells = BlockingOutboxDoorbellChannel(outbox)

scope.launch {
    runner.run(doorbells)
}
```

Do not run multiple batch readers for the same provider id. The runner keeps
one ordered drain path so `readNextBatch -> send -> ack` preserves cursor
semantics.

When multiple provider runners are active, have one coroutine collect the
blocking doorbell channel and fan out drain triggers. Doorbells are hints over
one native FD, not a broadcast stream per sink.

`write()` is intended for concurrent callers. Batch reads and ACKs are
serialized by the default client, but application code should still keep one
drain owner per provider id.

Use `noBackupFilesDir` or `filesDir` when pending records should survive normal
cache cleanup. Use `cacheDir` only when records may be discarded by the
operating system.

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

## Documentation

Read [docs/doc.md](docs/doc.md) for the design rationale, intended use cases,
doorbell/read/ACK model, sink orchestration rules, and crash logging boundary.
Multi-process apps can start from the
[IPC skeleton](docs/multi-process.md).

Manual test, native smoke, and stress diagnostic commands are in
[docs/testing.md](docs/testing.md).
Release notes are tracked in [CHANGELOG.md](CHANGELOG.md). Contribution
guidelines are in [CONTRIBUTING.md](CONTRIBUTING.md).

## Modules

- `:android-outbox`: publishable Maven AAR module. This is the core.
- `:app`: sample playground for manual Android experiments. It uses the local
  project dependency only so contributors can iterate quickly inside this repo.
  Real consumers should use the Maven coordinate above.

## Sample App

The sample app exists only to exercise the library manually. It is allowed to
use `implementation(project(":android-outbox"))` because both modules live in
the same source repository. That is a development convenience, not the intended
integration path for external apps.

Use the sample app for:

- writing records from UI actions
- checking flush/read/ACK flows
- simulating delivery failure by reading without ACK
- running lightweight burst experiments on a device

Keep heavy stress tests in the host-native test tasks. UI tests are useful for
manual behavior checks, but they should not become the primary performance
benchmark for the native outbox.

## Testing

Run the regular unit tests:

```bash
./gradlew :android-outbox:testDebugUnitTest --console=plain
```

Run lint and build the release AAR:

```bash
./gradlew :android-outbox:lintRelease :android-outbox:assembleRelease --console=plain
```

Native smoke, stress, host JNI, and shutdown race diagnostics are documented in
[docs/testing.md](docs/testing.md). They stay opt-in so the normal developer
and CI loop remains small.

## Build

Build the library module:

```bash
./gradlew :android-outbox:assembleDebug --console=plain
```

Build the sample app:

```bash
./gradlew :app:assembleDebug --console=plain
```

## License

AndroidOutBox is released under the MIT License. See [LICENSE](LICENSE).
