# Hexagonal Reactive Kafka Consumer Template — Design

Date: 2026-08-29
Status: Approved by user, pending implementation plan

## Purpose

A reusable Spring Boot template for building event-driven Kafka consumers.
It provides the *infrastructure* every consumer needs — lifecycle control,
metrics, structured error handling with retry/DLT, and OpenTelemetry-based
trace correlation — behind a strict hexagonal (ports & adapters) boundary,
verified by ArchUnit. It does **not** implement a specific business domain
or any persistence layer; the one concrete processor included
(`SampleMessageProcessor`) is a disposable example that proves the wiring
end-to-end.

## At a glance

| Area | Decision |
|---|---|
| Language / build | Java 21, Maven, single module |
| Framework | Spring Boot **3.5.x** (not the newer 4.1.x — ecosystem maturity) |
| Kafka client | Spring Kafka `@KafkaListener` (Reactor Kafka is discontinued [1]) |
| Message format | JSON (Jackson), no schema registry |
| Persistence | **None.** Postgres/R2DBC/Flyway dropped — YAGNI |
| Architecture | Hexagonal, single module, boundaries enforced by ArchUnit |
| Lifecycle control | `KafkaListenerEndpointRegistry`, behind a port, exposed via Actuator |
| Metrics & logging | Micrometer + SLF4J allowed in every layer — documented ArchUnit exception |
| Retry & dead-letter | `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` → `{topic}-dlt` |
| Concurrency | `@KafkaListener(concurrency=N)` — Kafka partitions, not thread pools or parallel streams |
| Reactive bridge | One `.block(Duration)` at the listener's edge only, BlockHound-guarded |
| Tracing | OpenTelemetry javaagent only — no custom correlation id, no tracing library dependency |

The rest of this document is the detail behind each row; the **Decision
Log** at the end has the full reasoning and sources for anything marked
with a number.

## Package structure (hexagonal, single module)

Base package: `com.template.batchconsumer` (rename freely per project).

```
com.template.batchconsumer
├── domain
│   ├── model        // MessageEnvelope, ProcessingOutcome, ConsumerStatus — pure data, no interfaces
│   └── exception    // RetryableProcessingException, NonRetryableProcessingException
├── application
│   ├── port
│   │   ├── in       // ConsumeMessageUseCase, ConsumerLifecycleUseCase, MessageProcessor<T>
│   │   └── out      // ConsumerLifecycleControlPort (start/stop/pause/resume/status by listener id)
│   ├── service      // ConsumerOrchestrationService (implements ConsumeMessageUseCase + ConsumerLifecycleUseCase)
│   └── sample       // SampleMessageProcessor implements MessageProcessor<T> — example only, delete-me
├── adapter
│   ├── in
│   │   ├── kafka    // @KafkaListener adapter — depends only on ConsumeMessageUseCase; bridges imperative -> reactive
│   │   └── web      // Actuator lifecycle endpoint — depends only on ConsumerLifecycleUseCase
│   └── out
│       └── kafka    // KafkaListenerLifecycleAdapter implements ConsumerLifecycleControlPort, wraps KafkaListenerEndpointRegistry
└── config           // composition root: @Bean wiring, KafkaConsumerConfig (error handler/backoff,
                       // DeadLetterPublishingRecoverer, per-listener `concurrency`), ObservabilityConfig (Micrometer)
```

Dead-letter handling is Kafka-only — the recoverer publishes to
`{topic}-dlt` and there is no DB audit record, since Postgres was dropped.

**Layer rules, plain language:**
- `domain` — pure business data + error taxonomy. Nothing else in the
  project may be imported here.
- `application` — owns every port (in and out) and the orchestration
  logic. `MessageProcessor<T>` (what concrete consumers implement) lives
  here alongside the operational ports.
- `adapter.in` — turns an external trigger (Kafka record, HTTP call) into
  a call against an application port. Never touches `adapter.out` directly.
- `adapter.out` — implements the outbound ports against real
  infrastructure (today: just the Kafka lifecycle-control adapter).
- `config` — the composition root. The only package allowed to see every
  layer at once; nothing depends on it in return.

## ArchUnit rules

Enforced in `HexagonalArchitectureTest`, run as a normal test:

| # | Rule |
|---|---|
| 1 | `domain` depends on nothing else in this project. |
| 2 | `application` depends only on `domain`, plus `io.micrometer..` / `org.slf4j..` (documented exception — see Decision Log). |
| 3 | `adapter.*` depends on `application` + `domain`, never the reverse. |
| 4 | `adapter.in` must not import `adapter.out.*` concrete classes — only `application.port.out` interfaces. |
| 5 | `config` is the top of the graph — may depend on everything; nothing may depend on it. |

## Cross-cutting mechanics

**Lifecycle (start/stop/pause/resume)** — `ConsumerOrchestrationService`
implements `ConsumerLifecycleUseCase` by calling `ConsumerLifecycleControlPort`,
which `KafkaListenerLifecycleAdapter` implements by wrapping
`KafkaListenerEndpointRegistry`. The port exists so `application` never
imports that Spring Kafka type directly.

**Ops surface** — custom Actuator endpoint `@Endpoint(id = "consumers")`:
- `GET /actuator/consumers` — status per consumer id (running/paused/stopped, last-poll time)
- `POST /actuator/consumers/{id}/{pause|resume|stop|start}`

**Metrics** (Micrometer, via `/actuator/prometheus`), recorded around the
`MessageProcessor` call:
- `consumer.messages.processed{consumer,outcome}` — counter
- `consumer.processing.duration{consumer}` — timer
- `consumer.messages.retried{consumer}` / `consumer.messages.dlt{consumer}` — counters

**Retry / dead-letter** — the listener bridges into the reactive
application layer with a single, outermost `.block(Duration)` (see
Decision Log) so Spring Kafka's `DefaultErrorHandler` can see exceptions
and apply backoff/DLT:
- `NonRetryableProcessingException` → straight to the recoverer, no retry
- `RetryableProcessingException` → `ExponentialBackOff` retries up to a configured max
- `DeadLetterPublishingRecoverer` publishes to `{topic}-dlt` — the sole record of failures (no DB, see Decision Log)

**Concurrency** — `@KafkaListener(concurrency=N)`, partition-based (one
thread per partition subset) — not a hand-rolled thread pool, not Java
parallel Streams (see Decision Log).

**Tracing** — no custom correlation-id code. Correlation is entirely
OpenTelemetry `traceId`/`spanId`, injected into MDC/logs by the external
OTel javaagent, which also auto-instruments Kafka producer/consumer spans
and Reactor's context propagation. No tracing library dependency is added
— correlation only shows up where the agent is attached (see Decision Log
for why, and for the caveat this design explicitly tests for).

## Testing

- **Unit tests** — `ConsumerOrchestrationService` logic against mocked ports.
- **ArchUnit test** — the five rules above, part of the normal test suite.
- **Blocking-safety test** — BlockHound (`blockhound-junit-platform`) fails
  the build if any blocking call happens on a Reactor-managed thread,
  guarding the single-blocking-point rule.
- **Integration tests** — Testcontainers (Kafka only): normal processing,
  and an induced failure asserting the `-dlt` topic receives the record
  with original headers intact.
- **OTel context-propagation test** — asserts, not assumes, that a span
  created inside the reactive body (after the `.block()` bridge) nests as
  a child of the Kafka consumer span, using OTel SDK's
  `InMemorySpanExporter`. Exists because this exact pattern has documented
  rough edges elsewhere (see Decision Log) — a regression here must fail
  CI, not surface as a broken trace in production.
- **Local dev** — `docker-compose.yml`, Kafka only (KRaft mode, no Zookeeper).

## Explicitly out of scope

- Concrete business domain beyond the disposable `SampleMessageProcessor`.
- Schema registry / Avro / Protobuf (JSON only).
- Multi-module Maven split (single module, package-based).
- JSON structured logging by default (documented follow-up if needed).
- HTTP business endpoints beyond the lifecycle-control actuator endpoint.
- Idempotency store, persistence layer, Postgres.
- Tracing library dependency (`micrometer-tracing-bridge-otel`) — local
  dev/tests show no trace correlation unless the OTel agent is attached.

## Decision Log

Short version of the reasoning behind each non-obvious call, in the order
they came up. Full citations are in References.

1. **Spring Kafka over Reactor Kafka.** Reactor Kafka is discontinued, and
   Spring's own reactive Kafka template is being deprecated too [1] — so
   `@KafkaListener` (imperative entry, reactive body) is the current
   supported shape, not a fallback.
2. **Metrics/logging bypass the port rule.** They're textbook
   cross-cutting concerns [2][3] — nobody swaps Micrometer/SLF4J for
   architectural reasons, and wrapping them in a port buys no testability.
   ArchUnit strictness stays focused on real infrastructure seams (Kafka).
3. **Single blocking bridge point.** `.block()` is only safe off a thread
   Reactor doesn't manage as non-blocking — a Kafka poll thread qualifies,
   a Netty/`Schedulers.parallel()` thread doesn't. Block exactly once, at
   the listener's outer edge, with a timeout; BlockHound [4] fails the
   build if anything inside the reactive body blocks.
4. **Kafka-partition concurrency, not parallel Streams.** Parallel Streams
   are for CPU-bound in-memory work and risk starving the JVM's shared
   `ForkJoinPool` if used for blocking I/O. `@KafkaListener(concurrency=N)`
   is the standard, partition-based scaling knob Kafka is designed around.
5. **No idempotency store, no Postgres.** Dropped per YAGNI — the user
   judged a full idempotency-claim mechanism too much surface for a
   template repo. Since Postgres's only purpose was idempotency + DLT
   audit, and audit alone doesn't justify a DB dependency, Postgres/R2DBC/
   Flyway are dropped entirely; the `-dlt` topic is the record of failures.
6. **"DLT," not "DLQ," and the current suffix.** Kafka has no queue
   primitive, so "Dead Letter Queue" is a misnomer — Spring Kafka's own API
   says "Dead Letter Topic." Also corrected: the default suffix changed
   from the legacy `.DLT` to `-dlt` as of Spring Kafka 3.3; this spec uses
   the current form throughout.
7. **Tracing via OpenTelemetry, not a custom correlation id.** A hand-rolled
   `X-Correlation-Id` + Reactor `Context` + `Hooks.enableAutomaticContextPropagation()`
   is a valid pattern [5] for services with no OTel agent — but once the
   user confirmed this template runs with the OTel javaagent attached, it
   became redundant: the agent already auto-instruments Kafka spans and
   Reactor context propagation [6], and `traceId`/`spanId` already land in
   MDC automatically [7]. The user also chose not to add
   `micrometer-tracing-bridge-otel` as a library dependency, so local
   dev/tests get no trace correlation unless the agent is attached — an
   accepted gap, traded for a zero-dependency POM.
8. **...but that bridge is tested, not assumed.** The OTel agent's Kafka
   and Reactor instrumentation modules are mature and built for exactly
   this shape, but Reactor context propagation under the agent has real,
   documented rough edges in other configurations [8][9]. Since any
   consumer built on this template could make an instrumented downstream
   reactive call from `MessageProcessor`, the risk is real — so the
   template includes an explicit span-parenting test instead of trusting
   the agent by assumption.

## References

1. [Reactor Kafka Project Will Be Discontinued — spring.io blog, 2025-05-20](https://spring.io/blog/2025/05/20/reactor-kafka-discontinued/)
2. [Cross-cutting concern — Wikipedia](https://en.wikipedia.org/wiki/Cross-cutting_concern)
3. [Hexagonal Architecture with Java and Spring — reflectoring.io](https://reflectoring.io/spring-hexagonal/)
4. [BlockHound — github.com/reactor/BlockHound](https://github.com/reactor/BlockHound)
5. [Context Propagation with Project Reactor 3 — spring.io blog, 2023-03-30](https://spring.io/blog/2023/03/30/context-propagation-with-project-reactor-3-unified-bridging-between-reactive/)
6. [OpenTelemetry Java instrumentation ecosystem](https://opentelemetry.io/docs/languages/java/instrumentation/)
7. [Observability with Spring Boot 3 — spring.io blog, 2022-10-12](https://spring.io/blog/2022/10/12/observability-with-spring-boot-3/)
8. [Reactor WebClient not using the span in context — OTel Java instrumentation #10011](https://github.com/open-telemetry/opentelemetry-java-instrumentation/issues/10011)
9. [Wrong parent span with Spring Cloud Gateway — OTel Java instrumentation #9495](https://github.com/open-telemetry/opentelemetry-java-instrumentation/issues/9495)
