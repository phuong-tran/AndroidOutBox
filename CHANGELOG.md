# Changelog

All notable project changes are documented here.

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
