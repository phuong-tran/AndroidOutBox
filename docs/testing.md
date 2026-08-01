# AndroidOutBox Testing

AndroidOutBox keeps the normal local feedback loop small. Regular Kotlin tests,
Android lint, and release assembly are the default local checks. Lower-level
native diagnostics use explicit tasks because they are more host-dependent; CI
runs smoke, JNI integration, ASan, and UBSan, while stress and shutdown-race
diagnostics remain opt-in.

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

## Host Sanitizers

Run the native smoke suite in two separate instrumented binaries: one with
AddressSanitizer (ASan) and one with UndefinedBehaviorSanitizer (UBSan).

```bash
./gradlew :android-outbox:testNativeHostSanitizers --console=plain
```

Run either sanitizer independently when diagnosing a failure:

```bash
./gradlew :android-outbox:testNativeHostAsan --console=plain
./gradlew :android-outbox:testNativeHostUbsan --console=plain
```

ASan stops on memory-safety errors and enables leak detection on Linux. UBSan
stops on the first detected undefined behavior. Both tasks first compile and
execute a small validation probe with an intentional fault, confirming that the
expected sanitizer catches it. If the compiler, sanitizer support, or runtime
is missing, the task fails with the detected reason and platform-specific
install commands instead of being silently skipped. Set `CC` to select another
host C compiler, for example:

```bash
CC=clang ./gradlew :android-outbox:testNativeHostSanitizers --console=plain
```

The host suite uses POSIX APIs. Windows users should run it inside WSL 2.

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

CI runs regular checks, the host-native smoke test, host JNI integration, and
both host sanitizers automatically. Stress and shutdown-race diagnostics remain
manual because they are substantially heavier or more host-specific. Use the
GitHub Actions manual workflow when you want those additional diagnostics.
