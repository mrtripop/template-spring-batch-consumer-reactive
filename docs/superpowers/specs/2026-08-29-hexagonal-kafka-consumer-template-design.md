# Hexagonal Reactive Kafka Consumer Template — Design

Date: 2026-08-29
Status: Approved by user, pending implementation plan

## Purpose

A reusable Spring Boot template for building event-driven Kafka consumers.
It provides the *infrastructure* every consumer needs — lifecycle control,
metrics, structured error handling with retry/DLT, idempotency, and
correlation-id propagation — behind a strict hexagonal (ports & adapters)
boundary, verified by ArchUnit. It does **not** implement a specific business
domain; the one concrete processor included (`SampleMessageProcessor`) is a
disposable example that proves the wiring end-to-end.

## Stack

- Java 21, Maven, single module
- Spring Boot 3.5.x (chosen over the newer 4.1.x line for ecosystem maturity —
  Spring Kafka, Testcontainers, R2DBC drivers, ArchUnit all have longer track
  records against 3.5.x)
- `spring-boot-starter-webflux`
- `spring-kafka` (`@KafkaListener`-based; Reactor Kafka was considered but
  Reactor Kafka's maintenance status made Spring Kafka the safer choice —
  the listener itself is imperative, but hands off into the reactive
  application layer)
- `spring-boot-starter-data-r2dbc` + `r2dbc-postgresql`
- `flyway-core` + `org.postgresql:postgresql` (JDBC driver — used only by
  Flyway at migration time; Flyway does not speak R2DBC)
- `spring-boot-starter-actuator` + `micrometer-registry-prometheus`
- No Lombok — plain Java records/classes
- Test scope: `archunit-junit5`, `testcontainers` (`postgresql`, `kafka`,
  `junit-jupiter`), `spring-kafka-test`

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
│   │   └── out      // IdempotencyStorePort, DeadLetterAuditPort
│   ├── service      // ConsumerOrchestrationService (implements ConsumeMessageUseCase + ConsumerLifecycleUseCase)
│   └── sample       // SampleMessageProcessor implements MessageProcessor<T> — example only, delete-me
├── adapter
│   ├── in
│   │   ├── kafka    // @KafkaListener adapter — depends only on ConsumeMessageUseCase
│   │   └── web      // Actuator lifecycle endpoint — depends only on ConsumerLifecycleUseCase
│   └── out
│       ├── persistence  // R2DBC entities/repos implementing IdempotencyStorePort, DeadLetterAuditPort
│       └── kafka        // DLT topic publisher adapter
└── config           // composition root: @Bean wiring, KafkaConsumerConfig (error handler/backoff/recoverer),
                       // R2dbcConfig, ObservabilityConfig (MDC + Micrometer)
```

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
  infrastructure (Postgres via R2DBC, Kafka DLT producer).
- **config**: the only place allowed to see every layer at once, since it is
  the Spring composition root wiring beans together. Nothing else may depend
  on `config`.

## ArchUnit rules (enforced in `HexagonalArchitectureTest`, run as a normal test)

1. `domain` depends on nothing else in this project.
2. `application` (including `application.sample`) depends only on `domain`.
3. `adapter.*` depends on `application` and `domain`, never the reverse.
4. `adapter.in` must not import `adapter.out.*` concrete classes — only
   `application.port.out` interfaces (prevents adapter-to-adapter calls that
   bypass the application layer, e.g. a Kafka error-handler recoverer wired
   directly to the persistence adapter instead of through
   `DeadLetterAuditPort`).
5. `config` is declared the top of the dependency graph: allowed to depend
   on all layers; no other package may depend on `config`.

## Cross-cutting mechanics

### Lifecycle (start/stop/pause/resume)

Backed by Spring Kafka's `KafkaListenerEndpointRegistry`, which already
supports per-container pause/resume/start/stop — no hand-rolled state
machine. `ConsumerOrchestrationService` implements `ConsumerLifecycleUseCase`
by delegating to the registry, keyed by listener id.

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

Spring Kafka's `DefaultErrorHandler` with `ExponentialBackOff`, classifying
by exception type:
- `NonRetryableProcessingException` → straight to the recoverer, no retry
- `RetryableProcessingException` → backed-off retries up to a configured max

The `DeadLetterPublishingRecoverer` publishes the failed record to
`{topic}.DLT` and, through the `DeadLetterAuditPort` interface (never the
concrete adapter — see ArchUnit rule 4), writes a `dead_letter_record` row
for inspection/replay.

### Idempotency

Before calling `MessageProcessor`, `ConsumerOrchestrationService` calls
`IdempotencyStorePort.tryClaim(messageKey)`, implemented via R2DBC as
`INSERT ... ON CONFLICT DO NOTHING`, checking rows-affected to atomically
detect a duplicate delivery.

Table: `idempotency_record(message_key PK, consumer_id, processed_at)`

### Tracing / logging correlation

The Kafka adapter reads an `X-Correlation-Id` record header (generates a
UUID if absent), places it in MDC before invoking the use case, and
propagates it into the DLT audit record and all log lines for that message.
Plain pattern-layout logging with `%X{correlationId}` by default — no JSON
encoder dependency added (easy to swap for `logstash-logback-encoder` later
if needed).

### Postgres schema (Flyway `V1__init.sql`)

- `idempotency_record(message_key PK, consumer_id, processed_at)`
- `dead_letter_record(id PK, consumer_id, topic, partition, offset, message_key, payload, exception_class, exception_message, correlation_id, failed_at)`

## Testing

- **Unit tests**: `ConsumerOrchestrationService` logic against mocked ports.
- **ArchUnit test**: `HexagonalArchitectureTest`, enforcing the five rules
  above via `layeredArchitecture()`, runs as part of the normal test suite.
- **Integration tests**: Testcontainers (Postgres + Kafka), full Spring
  context. Covers: normal processing (idempotency row written), duplicate
  delivery (second attempt short-circuited by idempotency check), and
  induced failure (DLT topic message + `dead_letter_record` row written).
- **Local dev**: `docker-compose.yml` with Postgres + Kafka (KRaft mode, no
  Zookeeper).

## Explicitly out of scope

- No concrete business domain beyond the disposable `SampleMessageProcessor`.
- No schema registry / Avro / Protobuf (JSON only, per decision).
- No multi-module Maven split (single module, package-based, per decision).
- No JSON structured logging by default (left as a documented follow-up).
- No HTTP-triggered business endpoints beyond the lifecycle-control actuator endpoint.
