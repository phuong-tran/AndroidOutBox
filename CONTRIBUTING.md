# Contributing

Thanks for taking a look at AndroidOutBox.

AndroidOutBox is intentionally small. Contributions should preserve the core
contract:

- bounded resources
- file-first persistence
- thin JNI
- provider-neutral native runtime
- no hidden network
- no dependency on DI, WorkManager, or vendor SDKs
- app-owned payloads, retry policy, and sinks

## Local Checks

Run the normal feedback loop before opening a PR:

```bash
./gradlew :android-outbox:testDebugUnitTest --console=plain
./gradlew :android-outbox:lintRelease --console=plain
./gradlew :android-outbox:assembleRelease --console=plain
```

Host-native diagnostics are manual-only and should be run when changing native
queue, file, cursor, frame, pipe, lifecycle, or ACK behavior:

```bash
./gradlew :android-outbox:testNativeHost \
  -PandroidOutboxHostNative=true \
  --console=plain

./gradlew :android-outbox:testDebugUnitTest \
  --tests "io.github.phuongtran.androidoutbox.OutboxHostJniIntegrationTest" \
  -PandroidOutboxHostJni=true \
  --console=plain

./gradlew :android-outbox:testDebugUnitTest \
  --tests "io.github.phuongtran.androidoutbox.OutboxHostJniShutdownRaceTest" \
  -PandroidOutboxHostJniRace=true \
  --console=plain

./gradlew :android-outbox:testNativeHostStress \
  -PandroidOutboxStress=true \
  --console=plain
```

## Diagnostic Output

Native diagnostics should stay provider-neutral. Prefer `primary` and
`secondary` over vendor names. The core outbox must not know which backend a
provider represents.

When clean JSON is needed for review:

```bash
./gradlew -q :android-outbox:testNativeHost \
  -PandroidOutboxHostNative=true | sed -n '/^{/,$p'
```

## Static Maven Publishing

This repository hosts its own static Maven repository under `maven/`.

To publish a new local static Maven version:

```bash
./gradlew :android-outbox:publishReleasePublicationToGitHubStaticMavenRepository \
  --console=plain
```

Commit the generated `maven/` changes together with the version bump.
