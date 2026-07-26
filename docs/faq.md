# AndroidOutBox FAQ

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

You may still want those products. Sentry, Datadog, Firebase Crashlytics,
Firebase Analytics, Bugsnag, New Relic, Embrace, OpenTelemetry-based clients,
and similar tools can be valuable once the app decides to send telemetry to
them.

AndroidOutBox solves a different problem: it gives the app an explicit,
bounded, app-owned handoff point before data enters any vendor SDK or network
pipeline.

Full observability SDKs often work by taking ownership of more runtime behavior:

- automatic exception, breadcrumb, lifecycle, network, or performance capture
- background queues and upload workers
- retry, batching, persistence, and rate-limit behavior hidden behind SDK calls
- extra payload enrichment such as device, session, user, and environment data
- network scheduling that may interact with app startup, foreground work, or
  low-battery conditions

Those features can be useful, but they also make reasoning harder. When app
startup becomes slower, battery drain increases, disk usage grows, network
traffic spikes, or privacy review asks why a field was sent, the answer may be
spread across SDK defaults, enabled integrations, background workers, remote
configuration, and app code.

AndroidOutBox keeps the first boundary under app control:

| Question | Vendor SDK First | AndroidOutBox First |
|---|---|---|
| What gets captured? | Often includes automatic SDK integrations. | Only records the app explicitly writes. |
| Who owns payload shape? | SDK defaults may enrich or transform data. | The app owns every payload field. |
| What happens under pressure? | Behavior depends on each SDK's queue, retry, and persistence policy. | Queue, record size, segment size, and retention limits are explicit. |
| Can logging affect the hot path? | SDK calls may hide work behind a small API surface. | `write()` is designed to stay off disk I/O on the caller hot path. |
| How is delivery committed? | SDK-specific. | Read, send, then ACK. |
| Can the app swap sinks later? | Often tied to vendor concepts. | The outbox is vendor-agnostic. |

This is not an argument that vendor SDKs are bad. It is an argument for keeping
the app's critical telemetry boundary explicit. An app can drain AndroidOutBox
to Sentry, Datadog, Firebase, its own backend, a file uploader, or several sinks
at once. The difference is that the app decides what crosses that boundary and
when.

## Why Does AndroidOutBox Drop Records?

Because a logging system should not take down the application it is observing.

AndroidOutBox is intentionally bounded. If the native queue is full, a record is
too large, or retained segments exceed configured limits, the library may reject
or discard telemetry so the app can keep running.

That trade-off is deliberate. Losing telemetry is bad; crashing, freezing, or
filling user storage because of telemetry is worse.

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
