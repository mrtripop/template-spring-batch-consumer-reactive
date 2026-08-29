# Hexagonal Reactive Kafka Consumer Template — Design

Date: 2026-08-29
Status: Approved by user, pending implementation plan

## Purpose

A reusable Spring Boot template for building event-driven Kafka consumers.
It provides the *infrastructure* every consumer needs — lifecycle control,
metrics, structured error handling with retry/DLT, and OpenTelemetry-based
trace correlation — behind a strict hexagonal (ports & adapters) boundary,
verified by ArchUnit. It does **not** implement a specific business domain;
the one concrete processor included (`SampleMessageProcessor`) is a
disposable example that proves the wiring end-to-end. It also does not
implement idempotency or any persistence — see Decision Log.

## Stack

- Java 21, Maven, single module
- Spring Boot 3.5.x (chosen over the newer 4.1.x line for ecosystem maturity —
  Spring Kafka, Testcontainers, ArchUnit all have longer track records
  against 3.5.x)
- `spring-boot-starter-webflux`
- `spring-kafka` (`@KafkaListener`-based — see Decision Log, "Spring Kafka
  over Reactor Kafka")
- `spring-boot-starter-actuator` + `micrometer-registry-prometheus`
- No Lombok — plain Java records/classes
- No PostgreSQL / R2DBC / Flyway — dropped, see Decision Log ("No
  idempotency store, no Postgres")
- Test scope: `archunit-junit5`, `testcontainers` (`kafka`,
  `junit-jupiter`), `spring-kafka-test`, `blockhound-junit-platform` (see
  Decision Log, "Single blocking bridge point")

Kafka message format: **JSON** (Jackson), no schema registry.

Base package: `com.template.batchconsumer` (rename freely per project).

## Package structure (hexagonal, single module)

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
│   │   ├── kafka    // @KafkaListener adapter — depends only on ConsumeMessageUseCase; bridges imperative->reactive (see Decision Log)
│   │   └── web      // Actuator lifecycle endpoint — depends only on ConsumerLifecycleUseCase
│   └── out
│       └── kafka    // KafkaListenerLifecycleAdapter implements ConsumerLifecycleControlPort, wrapping KafkaListenerEndpointRegistry
└── config           // composition root: @Bean wiring, KafkaConsumerConfig (error handler/backoff, DeadLetterPublishingRecoverer,
                       // per-listener `concurrency`), ObservabilityConfig (Micrometer)
```

Note: dead-letter handling is Kafka-only (publish to `{topic}.DLT` via
`DeadLetterPublishingRecoverer`) — there is no DB audit record. This is a
direct consequence of dropping Postgres (see Decision Log).

### Layer responsibilities

- **domain**: pure business data and error taxonomy. No ports, no Spring,
  no Kafka, no R2DBC types. May use `reactor.core.publisher` types if needed
  since Reactor is a general reactive-streams library, not Spring-specific —
  but plain domain types are preferred where reactivity isn't required.
- **application**: defines all ports (inbound and outbound) and implements
  the orchestration logic. `ConsumeMessageUseCase` is what the Kafka adapter
  drives; `ConsumerLifecycleUseCase` is what the web/actuator adapter drives.
  `MessageProcessor<T>` is the business-logic extension point that concrete
  consumers (like `SampleMessageProcessor`) implement — it lives in
  `application.port.in` alongside the other driving ports.
- **adapter.in**: translates an external trigger (Kafka record, HTTP call)
  into a call against an application port interface. Never depends on
  `adapter.out` concrete classes.
- **adapter.out**: implements the outbound ports against real
  infrastructure — here, just `KafkaListenerLifecycleAdapter` wrapping
  `KafkaListenerEndpointRegistry`.
- **config**: the only place allowed to see every layer at once, since it is
  the Spring composition root wiring beans together. Nothing else may depend
  on `config`.

## ArchUnit rules (enforced in `HexagonalArchitectureTest`, run as a normal test)

1. `domain` depends on nothing else in this project.
2. `application` (including `application.sample`) depends only on `domain`,
   plus `io.micrometer..` and `org.slf4j..` — an explicit, documented
   exception for cross-cutting observability concerns (see Decision Log,
   "Metrics/logging as an allowed exception"). No other third-party or
   Spring package may be imported by `application`.
3. `adapter.*` depends on `application` and `domain`, never the reverse.
4. `adapter.in` must not import `adapter.out.*` concrete classes — only
   `application.port.out` interfaces (prevents adapter-to-adapter calls that
   bypass the application layer, e.g. a lifecycle-control call reaching
   `KafkaListenerEndpointRegistry` directly instead of through
   `ConsumerLifecycleControlPort`).
5. `config` is declared the top of the dependency graph: allowed to depend
   on all layers; no other package may depend on `config`.

## Cross-cutting mechanics

### Lifecycle (start/stop/pause/resume)

Backed by Spring Kafka's `KafkaListenerEndpointRegistry`, which already
supports per-container pause/resume/start/stop — no hand-rolled state
machine. `ConsumerOrchestrationService` implements `ConsumerLifecycleUseCase`
by delegating to `ConsumerLifecycleControlPort` (`application.port.out`),
keyed by listener id. The port exists specifically so `application` never
imports `org.springframework.kafka.config.KafkaListenerEndpointRegistry`
directly — that type is implemented behind the port by
`adapter.out.kafka.KafkaListenerLifecycleAdapter`.

### Ops surface

A custom Actuator endpoint, `@Endpoint(id = "consumers")`:
- `GET /actuator/consumers` — status (running/paused/stopped, last-poll time) per consumer id
- `POST /actuator/consumers/{id}/{pause|resume|stop|start}` — drives `ConsumerLifecycleUseCase`

### Metrics (Micrometer, exposed via `/actuator/prometheus`)

Recorded in `ConsumerOrchestrationService` around the `MessageProcessor` call:
- `consumer.messages.processed{consumer,outcome}` — counter
- `consumer.processing.duration{consumer}` — timer
- `consumer.messages.retried{consumer}` — counter
- `consumer.messages.dlt{consumer}` — counter

### Retry / Dead-letter

The Kafka listener bridges into the reactive application layer via a
**single, outermost `.block(Duration)` call** — see Decision Log, "Single
blocking bridge point" — so that Spring Kafka's synchronous
`DefaultErrorHandler` can observe exceptions thrown from the reactive chain
and apply backoff/DLT. Classification is by exception type:
- `NonRetryableProcessingException` → straight to the recoverer, no retry
- `RetryableProcessingException` → `ExponentialBackOff` retries up to a
  configured max

The `DeadLetterPublishingRecoverer` publishes the failed record to
`{topic}.DLT`. There is no DB audit record (Postgres dropped, see Decision
Log) — the `.DLT` topic is the sole record of failures, replayable by
consuming it directly.

Per-consumer parallelism is via Spring Kafka's native `@KafkaListener(concurrency = ...)`
(partition-based, one thread per partition subset) — not a hand-rolled
thread pool, not Java parallel Streams (see Decision Log, "Concurrency via
Kafka partitions, not Java parallel Streams").

### Tracing / logging correlation

No custom correlation-id code — see Decision Log, "Tracing via OpenTelemetry,
not a custom correlation id". Correlation is entirely OpenTelemetry
`traceId`/`spanId`, injected into MDC and logs by the external OTel
javaagent (`-javaagent:opentelemetry-javaagent.jar`), which also
auto-instruments Kafka producer/consumer spans (extracting/injecting
`traceparent`/`tracestate` record headers) and Reactor's context
propagation, independent of any app-level wiring. The template adds no
tracing library dependency — trace correlation is only present where the
agent is actually attached (a deployed environment), not in local
`mvn spring-boot:run` or in tests, by explicit choice (see Decision Log).

## Testing

- **Unit tests**: `ConsumerOrchestrationService` logic against mocked ports.
- **ArchUnit test**: `HexagonalArchitectureTest`, enforcing the five rules
  above via `layeredArchitecture()`, runs as part of the normal test suite.
- **Blocking-safety test**: BlockHound wired into the test JVM
  (`blockhound-junit-platform`) to fail fast if any blocking call occurs on
  a Reactor-managed thread anywhere in the reactive chain, guarding the
  single-blocking-point invariant (see Decision Log).
- **Integration tests**: Testcontainers (Kafka only). Covers: normal
  processing, and an induced failure (asserting the `.DLT` topic receives
  the failed record with the original headers intact).
- **Local dev**: `docker-compose.yml` with Kafka (KRaft mode, no
  Zookeeper).

## Explicitly out of scope

- No concrete business domain beyond the disposable `SampleMessageProcessor`.
- No schema registry / Avro / Protobuf (JSON only, per decision).
- No multi-module Maven split (single module, package-based, per decision).
- No JSON structured logging by default (left as a documented follow-up).
- No HTTP-triggered business endpoints beyond the lifecycle-control actuator endpoint.
- No idempotency store, no persistence layer, no Postgres (see Decision Log).
- No tracing library dependency (`micrometer-tracing-bridge-otel`) — trace
  correlation relies solely on the external OTel javaagent, by choice (see
  Decision Log). Local dev/tests show no trace correlation unless the agent
  is attached.

## Decision Log

Each entry: the decision, why, and the source that grounded it (not just
asserted from training data — checked against current, dated references).

### Spring Kafka over Reactor Kafka

Reactor Kafka was discontinued by the Reactor team, confirmed directly from
Spring: [Reactor Kafka Project Will Be Discontinued — spring.io blog, 2025-05-20](https://spring.io/blog/2025/05/20/reactor-kafka-discontinued/).
That announcement also states the **Reactive template in Spring for Apache
Kafka is being deprecated and marked for removal** — so the fully-reactive
Kafka consumption path is being wound down by Spring itself, not merely
discouraged in third-party opinion. `@KafkaListener` (imperative entry,
reactive body) is therefore the current supported shape, not a fallback.

### Metrics/logging as an allowed ArchUnit exception

Logging and metrics are textbook cross-cutting concerns — they cut across
every layer rather than belonging to one, which is why SLF4J and Micrometer
are built as universal facades rather than domain-specific collaborators:
[Cross-cutting concern — Wikipedia](https://en.wikipedia.org/wiki/Cross-cutting_concern).
Practical hexagonal-architecture guidance treats cross-cutting concerns as
explicitly outside the ports/adapters routing:
[Hexagonal Architecture with Java and Spring — reflectoring.io](https://reflectoring.io/spring-hexagonal/).
The applied test: a port should isolate something you'd plausibly swap or
fake for a *business* reason (a database, a broker, an external API) —
nobody swaps Micrometer for architectural reasons, and it already ships a
no-op registry for tests, so wrapping it in a port buys no testability.
Scoping ArchUnit strictness to real infrastructure seams (Kafka) rather than
observability facades keeps the rule meaningful instead of teaching people
to route around it.

### Single blocking bridge point

`.block()` is only safe from a thread Reactor does **not** manage as
non-blocking (a Kafka consumer poll thread qualifies; a Netty event-loop or
`Schedulers.parallel()` thread does not — Reactor throws
`IllegalStateException` if you try there). The rule: block exactly once, at
the outermost edge (`@KafkaListener` method body), with a bounded timeout;
every layer inside the composed `Mono` chain (application, `MessageProcessor`)
stays fully reactive and never blocks itself. BlockHound
(https://github.com/reactor/BlockHound) is wired into tests to catch any
accidental blocking call sneaking into the reactive body, turning a silent
production risk into a failing test.

### Concurrency via Kafka partitions, not Java parallel Streams

Java parallel Streams (`ForkJoinPool.commonPool()`) are for CPU-bound,
in-memory bulk computation. Kafka consumption + processing is I/O-bound —
running blocking I/O inside `parallelStream()` doesn't add throughput and
risks starving the JVM's *shared* common pool used by unrelated code
elsewhere. The standard, large-scale approach is parallelism via Kafka
partitions: `@KafkaListener(concurrency = N)` runs N consumer threads, each
bound to a subset of partitions — this is how Kafka's own consumer-group
model is designed to scale, not a template-specific choice.

### No idempotency store, no Postgres

Dropped per YAGNI — the user judged a full idempotency-claim mechanism too
much surface for a template repo, to be added only when a concrete consumer
actually needs it. Since Postgres's only purpose in this design was
idempotency + DLT audit, and DLT audit alone doesn't justify a database
dependency for every consumer built on this template, Postgres/R2DBC/Flyway
are dropped entirely. The `.DLT` Kafka topic remains the record of failures.

### Tracing via OpenTelemetry, not a custom correlation id

Initially proposed: a hand-rolled `X-Correlation-Id` header + Reactor
`Context` + `Hooks.enableAutomaticContextPropagation()` (still a valid,
documented pattern — [Context Propagation with Project Reactor 3 — spring.io blog, 2023-03-30](https://spring.io/blog/2023/03/30/context-propagation-with-project-reactor-3-unified-bridging-between-reactive/)
— for services with no OTel agent). Superseded once the user confirmed this
template is expected to run with the OpenTelemetry javaagent attached: the
agent already auto-instruments Kafka producers/consumers (extracting and
injecting W3C Trace Context — `traceparent`/`tracestate` — record headers)
and has its own Reactor instrumentation module propagating trace context
across reactive operators, entirely independent of app code
([OpenTelemetry Java instrumentation ecosystem](https://opentelemetry.io/docs/languages/java/instrumentation/)).
When a span is active, `traceId`/`spanId` land in MDC automatically
([Observability with Spring Boot 3 — spring.io blog](https://spring.io/blog/2022/10/12/observability-with-spring-boot-3/)).
A custom correlation id alongside that would just be a second, redundant
string in every log line with none of the cross-service span linkage OTel
already provides — so it's dropped entirely.

The template deliberately adds no `micrometer-tracing-bridge-otel` library
dependency either (user's explicit choice) — correlation is only present
where the external javaagent is attached (a deployed environment), not in
local `mvn spring-boot:run` or tests. This is a real, accepted gap for
local/test observability, traded for a zero-dependency POM; revisit by
adding the library bridge if local trace correlation is later needed.
