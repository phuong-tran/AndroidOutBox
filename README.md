# AndroidOutBox

[![CI](https://github.com/phuong-tran/AndroidOutBox/actions/workflows/ci.yml/badge.svg)](https://github.com/phuong-tran/AndroidOutBox/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Version](https://img.shields.io/badge/version-1.3.4-blue.svg)](#installation)

AndroidOutBox is a small Android native-backed outbox for app-owned logs and
events.

It is not a Logcat reader. It is not a crash reporter. It is not an
observability SDK. It is not tied to Sentry, Datadog, Firebase, WorkManager, or
any backend.

AndroidOutBox protects the application first. Records are best-effort and may
be discarded under memory pressure, disk limits, retention limits, storage
cleanup, corruption, or other runtime constraints.

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

AndroidOutBox is experimental but usable. The public API is intentionally
small, and the runtime contract is documented and tested. Native/runtime
internals may still evolve while the project matures.

The project favors app safety over telemetry completeness. It is designed as a
bounded outbox, not an unbounded audit log.

## Delivery Model

AndroidOutBox is a bounded, best-effort local outbox designed to protect the
application first.

Records may be dropped under memory pressure, disk limits, retention limits,
storage cleanup, corruption, or other runtime constraints. Writes must never
block or destabilize the application.

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
implementation("io.github.phuongtran:android-outbox:1.3.4")
```

The AAR includes the Kotlin API and the native `libandroid_outbox.so` binaries
for Android ABIs. Gradle metadata, POM metadata, source jar, and checksums are
also committed under `maven/`.

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
doorbell/read/ACK model, and crash logging boundary.

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

Run the regular Kotlin/JVM unit tests:

```bash
./gradlew :android-outbox:testDebugUnitTest --console=plain
```

Run the host-native smoke test for the C core:

```bash
./gradlew :android-outbox:testNativeHost \
  -PandroidOutboxHostNative=true \
  --console=plain
```

Run the opt-in host JNI integration test:

```bash
./gradlew :android-outbox:testDebugUnitTest \
  --tests "io.github.phuongtran.androidoutbox.OutboxHostJniIntegrationTest" \
  -PandroidOutboxHostJni=true \
  --console=plain
```

These host tests compile and execute the native outbox core on the development
machine. The JNI integration path also builds a host-loadable shared library
from the production C/JNI objects, then validates cursor/ACK behavior, file
persistence, provider isolation, and JNI command framing without installing the
sample app on a device.

Run the host JNI shutdown race diagnostic separately:

```bash
./gradlew :android-outbox:testDebugUnitTest \
  --tests "io.github.phuongtran.androidoutbox.OutboxHostJniShutdownRaceTest" \
  -PandroidOutboxHostJniRace=true \
  --console=plain
```

Keep race and stress diagnostics opt-in. They are useful when changing
lifecycle, pipe, queue, file, cursor, frame, or ACK logic, but they should not
change the normal developer feedback loop.

Native stress tests are opt-in so normal development and CI do not pay the cost
unless explicitly requested:

```bash
./gradlew :android-outbox:testNativeHostStress \
  -PandroidOutboxStress=true \
  --console=plain
```

Use the stress task when you want a heavier confidence check for the native
producer/writer path.

Copy/paste these command lines when sharing manual review instructions:

```bash
./gradlew :android-outbox:testNativeHost \
  -PandroidOutboxHostNative=true \
  --console=plain

./gradlew :android-outbox:testNativeHostStress \
  -PandroidOutboxStress=true \
  --console=plain
```

When you need clean JSON for copy/paste, filter from the first JSON line:

```bash
./gradlew -q :android-outbox:testNativeHost \
  -PandroidOutboxHostNative=true | sed -n '/^{/,$p'
```

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
