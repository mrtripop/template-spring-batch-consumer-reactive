# template-spring-batch-consumer-reactive

A reusable Spring Boot template for building **reactive Kafka consumers** with a **hexagonal
(ports & adapters) architecture**, strictly enforced by ArchUnit. It ships with:

- Lifecycle control over running Kafka listener containers (pause / resume / stop / start) via a
  Spring Boot Actuator endpoint.
- Micrometer metrics for message processing, retries, and dead-letter events.
- Retry with exponential backoff and dead-letter topic (DLT) publishing, distinguishing
  retryable vs. non-retryable failures.
- OpenTelemetry context propagation across the reactive processing chain.
- A single, guarded blocking bridge point between Spring Kafka's imperative delivery model and
  the reactive processing pipeline (enforced by BlockHound).

For the full design rationale, decision log, and task-by-task history behind this template, see
[`docs/superpowers/specs/2026-08-29-hexagonal-kafka-consumer-template-design.md`](docs/superpowers/specs/2026-08-29-hexagonal-kafka-consumer-template-design.md).

## The disposable example slice

This template ships with a **worked example** consumer so the architecture can be seen end to
end. When adopting this template for a real consumer, **delete or replace these classes** —
they exist only to demonstrate the pattern:

| Class | Package | Purpose in the example |
|---|---|---|
| `SamplePayload` | `application.sample` | The example message payload record |
| `SampleMessageProcessor` | `application.sample` | The example `MessageProcessor<SamplePayload>` business logic |
| `SampleKafkaListenerAdapter` | `adapter.in.kafka` | The example `@KafkaListener` adapter (the single `.block()` bridge point) |

Everything else — `ConsumerOrchestrationService`, the ports in `application.port.*`,
`KafkaConsumerConfig`, `KafkaListenerLifecycleAdapter`, `ConsumerLifecycleEndpoint`, the domain
model — is reusable template infrastructure, not example code. A new consumer is added by writing
your own payload type, `MessageProcessor` implementation, and a listener adapter following
`SampleKafkaListenerAdapter`'s shape, then wiring it up alongside (or in place of) the sample
beans in `ApplicationConfig`/`KafkaConsumerConfig`.

## Running

This project uses a Maven wrapper — **always use `./mvnw`, never a bare `mvn`**, so the build
uses the exact Maven version this project was set up with.

```bash
# Run the full test suite (unit + ArchUnit + integration tests against a Testcontainers Kafka broker)
./mvnw test

# Start a local Kafka broker for manual/local running of the app
docker compose up

# Run the application itself (expects Kafka reachable at $KAFKA_BOOTSTRAP_SERVERS, default localhost:9092)
./mvnw spring-boot:run
```

The integration test suite (`SampleConsumerIntegrationTest`) requires Docker and spins up a real
Kafka broker via Testcontainers; it is slower than the unit tests (expect roughly 30-90+ seconds).

## Operational control: `/actuator/consumers`

The template exposes a custom Spring Boot Actuator endpoint for controlling running Kafka
listener containers at runtime, without redeploying:

| Method | Path | Effect |
|---|---|---|
| `GET` | `/actuator/consumers` | List the status of every registered consumer |
| `GET` | `/actuator/consumers/{consumerId}` | Get the status of one consumer |
| `POST` | `/actuator/consumers/{consumerId}/{action}` | Apply an action: `PAUSE`, `RESUME`, `STOP`, or `START` |

An unknown `consumerId` returns HTTP `404` (not `500`) on both the read and write operations.

### Security caveat — read before deploying anywhere

> **The `/actuator/consumers` write operations (PAUSE / STOP / RESUME / START) are exposed with
> NO authentication in this template.** Anyone who can reach the actuator port can pause or stop
> your consumers. This was a deliberate scope decision for this template — adding Spring Security
> was explicitly out of scope here — but it **must** be addressed before any real deployment.
>
> At minimum, before deploying:
> - Isolate the actuator port with `management.server.port` (a different port than the main
>   application, firewalled off from public access), and/or
> - Secure the actuator endpoints with authentication/authorization (e.g. Spring Security with a
>   role restricted to operators).

## Metrics

The template emits the following Micrometer metrics, all tagged with `consumer` (the consumer
id, e.g. `sample-consumer`):

| Metric | Tags | Meaning |
|---|---|---|
| `consumer.messages.processed` | `consumer`, `outcome` | Counter incremented per delivery attempt outcome (`SUCCESS`, `RETRYABLE_FAILURE`, `NON_RETRYABLE_FAILURE`) |
| `consumer.processing.duration` | `consumer` | Timer for how long each processing attempt took |
| `consumer.messages.retried` | `consumer` | Counter incremented once per retry attempt |
| `consumer.messages.dlt` | `consumer` | Counter incremented once per message published to the dead-letter topic |

**Note:** `consumer.messages.processed` counts *delivery attempts*, not unique messages — a
message that fails, gets retried, and eventually lands in the DLT increments this counter once
per attempt (including the eventual failure), not once overall. See "Retry/DLT semantics" below
for why a single message can be delivered more than once.

## The single-`.block()` invariant

Everything downstream of the Kafka listener is reactive (`Mono`/`Flux`) end to end. The **only**
place a blocking call is allowed in the whole reactive chain is the single, outermost bridge point
where Spring Kafka's imperative `@KafkaListener` delivery model hands off into the reactive
pipeline: `SampleKafkaListenerAdapter.onMessage`, which calls `.block(BLOCK_TIMEOUT)` exactly
once. This invariant is enforced automatically by `BlockHoundGuardTest` (via BlockHound), which
fails the build if a blocking call sneaks into a non-blocking scheduler anywhere else in the
chain.

## Retry / DLT semantics

Failures are handled by Spring Kafka's `DefaultErrorHandler` configured with an exponential
backoff (`consumer.sample.retry.*` in `application.yml`):

```yaml
consumer:
  sample:
    retry:
      max-attempts: 3          # retries AFTER the first delivery — 4 total deliveries, not 3
      initial-interval-ms: 1000
      multiplier: 2.0
      max-interval-ms: 10000
```

- `max-attempts: 3` means **3 retries after the first delivery attempt** — 4 total deliveries
  before the message is given up on, not 3 total attempts. Backoff between attempts follows the
  configured multiplier (1s, 2s, 4s, ...) up to `max-interval-ms`.
- Once retries are exhausted (or immediately, for a non-retryable failure), the message is
  published to a dead-letter topic via `DeadLetterPublishingRecoverer`. By convention the DLT is
  the source topic name with a `-dlt` suffix (e.g. `sample-events` -> `sample-events-dlt`).
- Throwing `NonRetryableProcessingException` from a `MessageProcessor` skips retries entirely and
  routes the message straight to the DLT on the first failure — use it for failures that will
  never succeed on retry (e.g. malformed business data), as opposed to transient failures
  (network blips, downstream outages) that should use the default retryable path.
- Malformed message payloads (values that don't deserialize as the expected type — a classic
  Kafka "poison pill") are handled the same way: `ErrorHandlingDeserializer` wraps the value
  deserializer so a deserialization failure surfaces as a normal error through this same
  error-handling/DLT path, instead of crashing the consumer's poll loop and wedging it on that
  offset forever.
