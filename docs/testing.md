# AndroidOutBox Testing

AndroidOutBox keeps the normal feedback loop small. Regular Kotlin tests,
Android lint, and release assembly are the default checks. Native smoke,
stress, JNI integration, and shutdown race diagnostics are opt-in because they
exercise lower-level runtime behavior and can be more host-dependent.

## Regular Checks

Run the Kotlin/JVM unit tests:

```bash
./gradlew :android-outbox:testDebugUnitTest --console=plain
```

Run Android lint:

```bash
./gradlew :android-outbox:lintRelease --console=plain
```

Build the release AAR:

```bash
./gradlew :android-outbox:assembleRelease --console=plain
```

Build the sample app:

```bash
./gradlew :app:assembleDebug --console=plain
```

## Host-Native Smoke

The host-native smoke test compiles and executes the C core on the development
machine. It validates queue pressure, large frames, provider cursor behavior,
restart retry, and ACK semantics without requiring an Android device.

```bash
./gradlew :android-outbox:testNativeHost \
  -PandroidOutboxHostNative=true \
  --console=plain
```

For clean JSON output:

```bash
./gradlew -q :android-outbox:testNativeHost \
  -PandroidOutboxHostNative=true | sed -n '/^{/,$p'
```

## Host JNI Integration

The host JNI integration test builds a host-loadable shared library from the
production C/JNI objects. It then validates pipe framing, file descriptor
ownership, cursor/ACK behavior, provider isolation, and restart behavior from a
plain JVM test.

```bash
./gradlew :android-outbox:testDebugUnitTest \
  --tests "io.github.phuongtran.androidoutbox.OutboxHostJniIntegrationTest" \
  -PandroidOutboxHostJni=true \
  --console=plain
```

## Shutdown Race

Run this when changing lifecycle, pipe close, command serialization, or native
shutdown behavior. It intentionally creates contention between write, flush,
read, ACK, stats, and close paths.

```bash
./gradlew :android-outbox:testDebugUnitTest \
  --tests "io.github.phuongtran.androidoutbox.OutboxHostJniShutdownRaceTest" \
  -PandroidOutboxHostJniRace=true \
  --console=plain
```

## Native Stress

Stress diagnostics are opt-in. They are useful when changing queue, writer,
segment rotation, retention, or producer hot-path logic, but they should not run
as part of the normal CI feedback loop.

```bash
./gradlew :android-outbox:testNativeHostStress \
  -PandroidOutboxStress=true \
  --console=plain
```

Useful knobs:

```bash
./gradlew :android-outbox:testNativeHostStress \
  -PandroidOutboxStress=true \
  -PandroidOutboxStressWorkers=8 \
  -PandroidOutboxStressRecordsPerWorker=20000 \
  -PandroidOutboxStressQueueCapacity=1024 \
  -PandroidOutboxStressMaxRecordBytes=256 \
  --console=plain
```

## CI Policy

CI runs regular checks automatically. Native diagnostics remain manual so
ordinary changes do not pay for stress or host-specific race tests. Use the
GitHub Actions manual workflow when you want host-native diagnostics from CI.
