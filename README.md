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

## Installation

AndroidOutBox is intended to be consumed as a Maven AAR.

Add the Maven repository in `settings.gradle.kts` or the application Gradle
configuration:

```kotlin
dependencyResolutionManagement {
    repositories {
        google()
        mavenCentral()
        maven {
            url = uri("https://maven.pkg.github.com/phuong-tran/AndroidOutBox")
            credentials {
                username = providers.gradleProperty("gpr.user")
                    .orElse(providers.environmentVariable("GITHUB_ACTOR"))
                    .orNull
                password = providers.gradleProperty("gpr.key")
                    .orElse(providers.environmentVariable("GITHUB_TOKEN"))
                    .orNull
            }
        }
    }
}
```

Then add the dependency:

```kotlin
implementation("io.github.phuongtran:android-outbox:1.3.1")
```

The published AAR includes the Kotlin API, Gradle metadata, sources, and the
native `libandroid_outbox.so` binaries for Android ABIs.

## Maven Publishing

For local-only integration testing, publish to Maven local and consume the same
coordinate from `mavenLocal()`:

```bash
./gradlew :android-outbox:publishReleasePublicationToMavenLocal --console=plain
```

```kotlin
repositories {
    mavenLocal()
    google()
    mavenCentral()
}
```

To publish to GitHub Packages, provide credentials through environment
variables or Gradle properties:

```bash
GITHUB_ACTOR=<github-user> \
GITHUB_TOKEN=<github-token> \
./gradlew :android-outbox:publishReleasePublicationToGitHubPackagesRepository --console=plain
```

The token needs permission to publish packages for this repository.

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

- `:android-outbox`: publishable Maven library module. This is the core.
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

## Useful Commands

```bash
./gradlew :android-outbox:testNativeHost
./gradlew :android-outbox:testDebugUnitTest
./gradlew :android-outbox:assembleDebug
./gradlew :android-outbox:publishReleasePublicationToMavenLocal --console=plain
./gradlew :app:assembleDebug --console=plain
```

Native stress tests are opt-in:

```bash
./gradlew :android-outbox:testNativeHostStress -PandroidOutboxStress=true --console=plain
```
