# AndroidOutBox FAQ

## Table Of Contents

- [Is AndroidOutBox Like Nginx For Mobile Logging?](#is-androidoutbox-like-nginx-for-mobile-logging)
- [Why Use AndroidOutBox If I Already Use Sentry, Datadog, Firebase, Or Another SDK?](#why-use-androidoutbox-if-i-already-use-sentry-datadog-firebase-or-another-sdk)
- [Why Does AndroidOutBox Drop Records?](#why-does-androidoutbox-drop-records)
- [Do Other SDKs Drop Records Too?](#do-other-sdks-drop-records-too)
- [Does Best-Effort Mean AndroidOutBox Is Unreliable?](#does-best-effort-mean-androidoutbox-is-unreliable)
- [Is `forceSync()` Required For Normal Logging?](#is-forcesync-required-for-normal-logging)
- [Should The Multi-Process Skeleton Handle Binder Reconnect?](#should-the-multi-process-skeleton-handle-binder-reconnect)
- [Should Every Process Create Its Own AndroidOutBox?](#should-every-process-create-its-own-androidoutbox)

## Is AndroidOutBox Like Nginx For Mobile Logging?

In one useful sense, yes.

Nginx often protects backend application servers from unbounded traffic
pressure. It limits request size, connection count, timeouts, buffering, and
other resource costs before those costs reach the application server.

AndroidOutBox plays a similar defensive role for app-owned mobile telemetry. It
puts a bounded handoff point in front of the logging or delivery pipeline:

| Backend Edge | Mobile Telemetry |
|---|---|
| Nginx protects the backend from traffic pressure. | AndroidOutBox protects the app from logging pressure. |
| Nginx bounds request size, connections, buffering, and timeouts. | AndroidOutBox bounds queue depth, record size, segment size, and retained segments. |
| Nginx may reject traffic when limits are reached. | AndroidOutBox may drop telemetry when limits are reached. |
| Nginx does not make backend processing lossless. | AndroidOutBox does not make telemetry delivery lossless. |
| Nginx keeps the main service alive under pressure. | AndroidOutBox keeps the host app safe under logging pressure. |

The analogy has limits. AndroidOutBox is not a reverse proxy, network server,
or policy engine. It does not decide what records mean, where they are sent, or
how the app retries delivery. The app owns those choices.

The useful comparison is the defensive boundary: logging should not be allowed
to consume unbounded memory, disk, CPU, or latency on the app's critical path.

## Why Use AndroidOutBox If I Already Use Sentry, Datadog, Firebase, Or Another SDK?

Because the first SDK is rarely the last one.

This is a common production shape:

1. The app already has Firebase for crashes, ANRs, and analytics.
2. A platform team asks for Sentry, Datadog, or New Relic for dashboards.
3. A product team asks for Firebase Analytics or another event stream.
4. A backend team asks for OpenTelemetry-style traces.
5. A compliance or support team asks for a private upload path.

Each request may be reasonable in isolation. The problem is what happens when
every tool wants to sit directly inside the app runtime.

Now the app may have two SDKs watching crashes and ANRs, two SDKs observing
lifecycle, two SDKs buffering records, two SDKs retrying uploads, and two SDKs
trying to explain the same session in their own vocabulary. Neither SDK knows
it is the secondary one. The app is the place where those overlapping
assumptions meet.

The team may also end up with two sources of truth. A crash in Firebase may need
to be mapped to an issue in Sentry, a trace in Datadog, a release in CI, and a
user or session in the app's backend. If those identifiers, sampling rules, and
payload fields do not line up, each dashboard can tell a slightly different
story about the same failure.

For many Android apps, crash and ANR visibility is already covered well by the
Google stack through Firebase and Play Console. If the organization still wants
those signals in Sentry, Loki, Datadog, or an internal system, that does not
automatically mean millions of app installs should pay for another crash/ANR
runtime. Often the cleaner boundary is an adapter or sink that forwards the
app-owned record after capture, where cost and policy are easier to control.

For example, Sentry can be treated as a sink instead of a second in-app crash
owner. The app can capture one app-owned record, drain it once, and have a
sink or backend adapter translate that payload into the destination protocol,
such as Sentry envelopes, Loki log streams, or an internal ingestion endpoint.
At the app boundary, one entrypoint that accepts the payload is usually enough;
the vendor-specific mapping can live downstream.

Full observability SDKs often bring their own capture, queues, persistence,
retry, enrichment, background work, network scheduling, and remote defaults. As
more SDKs are added, the app can end up with several components observing the
same lifecycle, recording similar events, waking background workers, buffering
payloads, and uploading on their own schedules.

That makes simple questions harder to answer:

| Question | With Many SDKs On The Hot Path | With AndroidOutBox As The Boundary |
|---|---|---|
| Who decided this event should exist? | It may come from app code, auto-capture, an integration, or remote defaults. | The app wrote the record explicitly. |
| Why did startup get slower? | Several SDKs may initialize, inspect state, or install hooks. | The first handoff is a small bounded write path. |
| Why did battery or network usage increase? | Upload workers, retries, and batching policies may overlap. | The app decides which sinks drain and when. |
| Why is disk usage growing? | Each SDK may own a private cache or queue. | Queue, record size, segment size, and retention are explicit. |
| Why was this field sent? | Payload enrichment may happen inside SDK defaults. | Payload shape is app-owned. |
| Which dashboard is the source of truth? | Crash, trace, session, and release ids may need manual mapping across tools. | The app writes one record shape and can fan it out to multiple sinks. |
| Who pays for duplicate crash and ANR capture? | Every installed app instance pays in runtime hooks, caches, and uploads. | The app can capture once and adapt downstream. |
| Do we need another in-app runtime just to reach a destination? | The destination often arrives as another SDK. | The destination can be treated as a sink or adapter. |
| How do we add a second destination? | Add another SDK or another SDK integration point. | Add another provider cursor and sink. |
| How is delivery committed? | SDK-specific and often hidden. | Read, send, then ACK. |

AndroidOutBox does not replace every vendor product. It gives the app one
controlled telemetry boundary before any vendor or backend receives data. From
there, the app can drain to Sentry, Datadog, Firebase, Bugsnag, New Relic,
Embrace, an OpenTelemetry collector, its own backend, or several destinations
at once.

The point is not "never use vendor SDKs." The point is to avoid letting every
new telemetry requirement install another independent runtime inside the app's
critical path. AndroidOutBox keeps capture, payload shape, pressure limits, and
delivery commit policy explainable.

## Why Does AndroidOutBox Drop Records?

Because a logging system should not take down the application it is observing.

AndroidOutBox is intentionally bounded. If the native queue is full, a record is
too large, or retained segments exceed configured limits, the library may reject
or discard telemetry so the app can keep running.

That trade-off is deliberate. Losing telemetry is bad; crashing, freezing, or
filling user storage because of telemetry is worse.

## Do Other SDKs Drop Records Too?

In practice, yes. There are physical and runtime reasons no mobile telemetry
SDK can fully avoid: disk full, process death while writing, power loss before
data reaches stable storage, OS cleanup, corruption, network failure, and
backend rejection.

Then there are SDK policy reasons. A full SDK may reject, sample, evict,
coalesce, rate-limit, or retry data behind its own queue, cache, uploader,
sampler, retention policy, remote configuration, or backend ingestion limits.
That does not make the data loss disappear. It only moves the decision into a
component the app may not fully control.

AndroidOutBox makes the trade-off explicit:

- if the in-memory queue is full, the record can be dropped
- if the record is too large, it can be rejected
- if retained segments exceed configured limits, old records can be removed
- if the app does not ACK, delivery can retry while the record remains retained

That is not a weaker contract than pretending loss never happens. It is a
contract the app can reason about.

## Does Best-Effort Mean AndroidOutBox Is Unreliable?

No. It means the contract is explicit.

No bounded mobile logging SDK can promise that every record survives every
runtime failure window. Power loss, process death during a write, OS storage
cleanup, filesystem corruption, quota pressure, and retention limits can affect
any mobile logging pipeline.

AndroidOutBox does not claim impossible guarantees. It provides:

- fast, app-safe writes
- bounded memory and disk usage
- retryable read/ACK delivery while records remain retained
- pressure counters through `getStats()`
- optional `forceSync()` when the app chooses to pay for a storage barrier

## Is `forceSync()` Required For Normal Logging?

No.

`flush()` waits for accepted records to reach the spool writer. `forceSync()`
asks the OS to sync the active segment to stable storage. That storage barrier
is intentionally separate because it can be expensive.

Most apps should keep normal logging on the fast path and reserve
`forceSync()` for explicit lifecycle or memory-pressure policies, if they want
that trade-off at all.

## Should The Multi-Process Skeleton Handle Binder Reconnect?

No.

The multi-process document is an ownership skeleton, not an Android service
lifecycle tutorial. Its critical invariant is simple: only one process should
open the native outbox and spool directory.

Binder reconnect, process priority, foreground/background policy, backoff,
permissions, and lifecycle behavior are app-specific. A generic reconnect
sample would be easy to copy into the wrong architecture. Apps should implement
that policy around their own process model.

## Should Every Process Create Its Own AndroidOutBox?

Not against the same spool directory.

For multi-process apps, either keep one owner process and expose the outbox
through app-private IPC, or use separate spool directories per process. Multiple
processes must not independently open and mutate the same spool directory.
