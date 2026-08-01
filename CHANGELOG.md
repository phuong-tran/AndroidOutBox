# Changelog

All notable project changes are documented here.

## Unreleased

- Kept concurrent writes on the ordered pipe hot path without waiting behind
  command/response operations; JNI remains a thin file-descriptor bridge.
- Added explicit queue, record, batch, frame, segment, and aggregate memory/disk
  safety ceilings on both Kotlin and native boundaries.
- Added deterministic malformed-frame/parser coverage and native filesystem
  fault injection for append, sync, cursor commit, and segment rotation paths.
- Added ThreadSanitizer host coverage beside ASan and UBSan; the new coverage
  found and fixed a control-thread lifecycle leak.

## 1.3.9 - 2026-07-30

- Published immutable static Maven artifacts under `maven/` for `1.3.9` and restored `1.3.8` to its original artifact checksum.
- Added Kotlin-side payload/category preflight so oversized writes are rejected before command-frame allocation or pipe handoff.
- Included Kotlin-side preflight drops in `OutboxStats`.
- Added consumer ProGuard rules to keep JNI entry points stable when apps minify dependencies.
- Added CI coverage for host-native smoke and host-JNI integration, plus an append-only guard for published Maven version directories.
- Documented single-reader doorbell fan-out expectations, provider cursor limits, and write handoff semantics.
- Removed tracked `.idea` state and fixed Linux host C warning failures under `-Werror`.

## 1.3.8 - 2026-07-26

- Added explicit `forceSync()` APIs for apps that choose an OS-level active-segment storage barrier.
- Kept `flush()` as a Java-style writer drain without per-record `fsync` on the hot append path.
- Added POSIX-oriented `fsync` retry and directory sync handling for spool file creation and cursor commits.
- Updated the sample app runtime panel to show separate `Flush` and `Force sync` actions.
- Published static Maven artifacts under `maven/` for `1.3.8`.

## 1.3.7 - 2026-07-24

- Added a lambda-backed `AndroidOutboxSinkRunner(...)` factory for composition-first sink setup.
- Kept direct `AndroidOutboxSinkRunner` extension available for custom doorbell filtering.
- Moved detailed native/JNI/stress diagnostic commands from README to `docs/testing.md`.
- Published static Maven artifacts under `maven/` for `1.3.7`.

## 1.3.6 - 2026-07-24

- Removed an unused coroutine import from `AndroidOutboxSinkRunner`.
- Published static Maven artifacts under `maven/` for `1.3.6`.

## 1.3.5 - 2026-07-24

- Added `AndroidOutboxSinkRunner` to centralize one ordered drain path per provider cursor.
- Added `BlockingOutboxDoorbellChannel` as the default coroutine-facing doorbell bridge.
- Added `OutboxDrainResult` for drain-pass summaries.
- Updated documentation to clarify sink orchestration and single-reader cursor ownership.
- Published static Maven artifacts under `maven/` for `1.3.5`.

## 1.3.4 - 2026-07-24

- Added provider-neutral native diagnostic JSON for host smoke and stress tests.
- Added per-provider read and ACK results to native diagnostics.
- Kept host-native smoke, stress, JNI, and race diagnostics manual-only.
- Reduced default diagnostic payload pressure while keeping large-frame coverage.
- Published static Maven artifacts under `maven/` for `1.3.4`.

## 1.3.3 - 2026-07-24

- Published AndroidOutBox as a static Maven AAR.
- Added README guidance for installation and manual testing.
- Added sample app improvements for write, read, ACK, and failure simulation.

## Earlier Releases

Earlier versions were used to establish the native-backed outbox baseline,
sample app, and static Maven publishing flow.
