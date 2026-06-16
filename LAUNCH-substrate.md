# AbsoluteJS substrate complete (G1–G7) launch plan

Draft social copy for the consolidated substrate-audit milestone. Local
file, untracked. Edit and post on your timeline.

## Core links

- Substrate-complete page: https://absolutejs.com/documentation/substrate-audit
- Audit deep-dive: https://absolutejs.com/documentation/audit-overview
- Telemetry deep-dive: https://absolutejs.com/documentation/telemetry-overview
- Dispatch deep-dive: https://absolutejs.com/documentation/dispatch-overview
- Cluster bus deep-dive: https://absolutejs.com/documentation/cluster-bus-overview
- Replay (vs Convex): https://absolutejs.com/documentation/sync-vs-convex#replay
- Migration (vs Convex): https://absolutejs.com/documentation/sync-vs-convex#migrate
- Sync 1.24 README: https://github.com/absolutejs/sync

## HN draft

Title:

```text
Show HN: AbsoluteJS substrate – every gap from our public audit is now closed
```

Body:

```text
Mid-2026 a deep-research audit of the AbsoluteJS substrate named seven cross-cutting gaps blocking the "can absolutejs.ai host other teams' production tenants" question. As of this week every gap is addressed in-tree.

The packages and what shipped against each:

- G1 Cross-surface audit log – @absolutejs/audit 0.0.1, with @absolutejs/audit-elysia and @absolutejs/audit-pg siblings. Append-only events with open `kind: string` shape, optional hash-chain integrity (SHA-256 or HMAC-SHA256), live-wire helpers for runtime/queue/secrets/sync.

- G2 OpenTelemetry across the substrate – @absolutejs/telemetry 0.0.3. Type-replicated OTel surface so we don't peer-dep @opentelemetry/api on every consumer. Sync, queue, runtime, router, secrets, rate-limit, isolated-jsc all emit spans via tracerOrNoop().

- G3 Stripe meter sink – declined explicitly. @absolutejs/metering was already pluggable; a Stripe sink belongs in the host control plane, not the substrate. The only gap closed by "no, we don't need to ship that."

- G4 Outbound dispatch – @absolutejs/dispatch 0.0.1, with vendor adapter siblings @absolutejs/dispatch-resend, dispatch-postmark, dispatch-twilio. Email/SMS/push behind one factory; vendor SDKs as true peer deps via narrow ClientLike interfaces.

- G5 Multi-region cluster bus – @absolutejs/sync-bus-redis 0.0.1 sibling to the existing sync-bus-pg. Same ClusterBus contract; swap is one constructor change. Redis pub/sub gives native geo-replication on managed offerings.

- G6 Point-in-time replay – sync 1.22 ships engine.replayTo({ at, tables? }) walking the bounded change log forward to a target timestamp. Sync 1.23 added a clickable Replay panel in syncDevtools so it's a 10-second demo, not a "trust me" claim.

- G7 Tenant migration primitives – sync 1.24 ships three composable verbs (engine.fence, exportSnapshot, importSnapshot) rather than a monolithic migrate(). Reads stay open under fence so live subscribers don't go dark during the transfer.

What is NOT in-tree, on purpose: managed deployment, strict determinism, managed multi-region failover. All host-operator concerns that belong to absolutejs.ai, not the library.

Consolidated record: https://absolutejs.com/documentation/substrate-audit
```

## Reddit draft (r/typescript, r/node, r/programming)

```text
AbsoluteJS substrate audit – all seven cross-cutting gaps closed in-tree

Earlier this year a deep-research audit of the AbsoluteJS substrate named seven gaps blocking production multi-tenancy. As of this week every gap is addressed.

- G1 audit log → @absolutejs/audit with hash-chain integrity
- G2 OTel → @absolutejs/telemetry, no @opentelemetry/api peer dep
- G3 Stripe meter sink → declined; metering was already pluggable
- G4 outbound dispatch → @absolutejs/dispatch + Resend/Postmark/Twilio adapters
- G5 cluster bus → @absolutejs/sync-bus-redis sibling to sync-bus-pg
- G6 point-in-time replay → sync 1.22 engine.replayTo + 1.23 Replay panel
- G7 tenant migration → sync 1.24 fence + exportSnapshot + importSnapshot

What's still NOT in-tree, by design: managed deployment, strict determinism, managed multi-region failover. Those are host-operator concerns and live in the absolutejs.ai PaaS, not the library.

Consolidated record:
https://absolutejs.com/documentation/substrate-audit
```

## Twitter/X thread draft

```text
The AbsoluteJS substrate audit is closed.

Earlier this year a deep-research audit named seven cross-cutting gaps blocking production multi-tenancy. As of this week every gap is addressed in-tree.

Each is a separate package or sync release. Walking them:

1/9

G1 — cross-surface audit log.

@absolutejs/audit 0.0.1.

Open kind: string shape, append fan-out to many sinks, optional hash-chain integrity (SHA-256 or HMAC).

Live-wire helpers attach to runtime/queue/secrets/sync without reaching into their lifecycles.

2/9

G2 — OpenTelemetry across the substrate.

@absolutejs/telemetry 0.0.3.

Type-replicated OTel surface so we don't peer-dep @opentelemetry/api on every consumer. tracerOrNoop() is the single entry. ABS_ATTRS is shared semantic conventions.

Sync, queue, runtime, router, secrets, rate-limit, isolated-jsc all emit spans.

3/9

G3 — Stripe meter sink.

Declined. @absolutejs/metering was already pluggable; a Stripe sink belongs in the host control plane, not the substrate.

The only G-gap closed by "no, we don't need to ship that."

4/9

G4 — outbound message dispatch.

@absolutejs/dispatch 0.0.1.

Email/SMS/push behind one factory. Three first-party vendor adapters (Resend, Postmark, Twilio) each a separate npm with the SDK as a true peer dep via narrow ClientLike interfaces.

5/9

G5 — multi-region cluster bus.

@absolutejs/sync-bus-redis 0.0.1, sibling to the existing sync-bus-pg.

Same ClusterBus contract; swap is one constructor change. Redis pub/sub gives native geo-replication on managed offerings (Cluster, ElastiCache Global, Memorystore, Upstash).

6/9

G6 — point-in-time replay.

Sync 1.22 ships engine.replayTo({ at, tables? }) walking the bounded change log forward to a target timestamp.

Sync 1.23 added a clickable Replay panel in syncDevtools. It's a 10-second demo, not a "trust me" claim.

7/9

G7 — tenant migration primitives.

Sync 1.24: three composable verbs (engine.fence / exportSnapshot / importSnapshot) rather than a monolithic migrate().

Reads stay open under fence so live subscribers don't go dark during the transfer.

8/9

What is NOT in-tree, by design:

— managed deployment
— strict determinism
— managed multi-region failover

All host-operator concerns. They belong to absolutejs.ai, not the library.

Consolidated record:
https://absolutejs.com/documentation/substrate-audit

9/9
```

## LinkedIn / blog summary (one-liner pulls)

```text
"Earlier this year a deep-research audit named seven cross-cutting gaps in our substrate. As of this week every gap is addressed in-tree."

"The only G-gap closed by 'no, we don't need to ship that': Stripe meter sink. Metering was already pluggable; a Stripe sink belongs in the host control plane, not the substrate."

"Why three verbs instead of one migrate(): transport is the operator's choice (S3? Kafka? gRPC?), and the strictness vs availability tradeoff is policy. The substrate offers the verbs; the choreography is yours."

"What is NOT in-tree, by design: managed deployment, strict determinism, managed multi-region failover. All host-operator concerns. They belong to the absolutejs.ai PaaS, not the library."
```

## Pre-post checklist

- Verify npm versions on the day of post:
    - `@absolutejs/sync` = `1.24.0`
    - `@absolutejs/audit` = `0.0.1`
    - `@absolutejs/telemetry` = `0.0.3`
    - `@absolutejs/dispatch` = `0.0.1`
    - `@absolutejs/dispatch-resend` = `0.0.1`
    - `@absolutejs/dispatch-postmark` = `0.0.1`
    - `@absolutejs/dispatch-twilio` = `0.0.1`
    - `@absolutejs/sync-bus-redis` = `0.0.1`
    - `@absolutejs/queue-redis` = `0.0.1`
- Confirm https://absolutejs.com/documentation/substrate-audit renders and the
  in-page anchors (#g1 .. #g7) all scroll.
- Confirm examples.absolutejs.com (or local sync demo) shows the Devtools ↗
  link and the Replay panel still works end-to-end.
- For HN: queue the post for a weekday morning ET; pin the substrate-audit
  link in the body, not the title. Title is bait-free.
- For Twitter: post the thread as a single composer-chain rather than split
  replies so the count-out displays correctly.
- For Reddit: r/typescript and r/node both allow links; r/programming has
  stricter rules — write a 200-word self-text version if posting there.
