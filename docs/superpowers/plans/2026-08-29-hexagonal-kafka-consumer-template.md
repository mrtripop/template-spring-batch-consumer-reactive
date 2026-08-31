# Hexagonal Reactive Kafka Consumer Template Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a reusable Spring Boot template for hexagonal, WebFlux/Reactor-based Kafka consumers — lifecycle control, metrics, retry/DLT, and OpenTelemetry tracing — with ArchUnit strictly enforcing the layer boundaries.

**Architecture:** Single-module Maven project, packages `domain` / `application` / `adapter.{in,out}` / `config`. A `@KafkaListener` (imperative) bridges into a reactive (`Mono`) application core via one bounded `.block()` call; Spring Kafka's `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` handle retry/DLT from outside that bridge.

**Tech Stack:** Java 21, Maven, Spring Boot 4.1.1, Spring WebFlux, Spring Kafka, Spring Boot Actuator, Micrometer (+ Prometheus registry), ArchUnit, Testcontainers (Kafka), BlockHound, OpenTelemetry SDK + Reactor instrumentation (test-only).

**Spec:** `docs/superpowers/specs/2026-08-29-hexagonal-kafka-consumer-template-design.md`

## Global Constraints

- Java 21 minimum.
- Spring Boot **4.1.1** — supersedes the spec's 3.5.x choice: 3.5.x reached end-of-life 2026-06-30 (final patch 3.5.16), confirmed with the user during planning; 4.1.1 is the current supported line.
- Base package: `com.template.batchconsumer`.
- No Lombok — plain Java records/classes.
- No PostgreSQL / R2DBC / Flyway, no idempotency store (spec Decision Log #5).
- Hexagonal layers (`domain`, `application`, `adapter.in`, `adapter.out`, `config`) — dependency direction strictly enforced by ArchUnit (Task 3); every later task's code must satisfy those rules.
- `application` may depend on `domain` plus `io.micrometer..` / `org.slf4j..` only — no Spring, no Kafka client, no R2DBC (spec Decision Log #2).
- DLT topic suffix is `-dlt` (current Spring Kafka default), not the legacy `.DLT` (spec Decision Log #6).
- Exactly one blocking call (`.block(Duration)`) per message, at the `@KafkaListener` method's outer edge only — never inside the reactive body (spec Decision Log #3).
- Tracing is OpenTelemetry-only — no custom correlation id, no `micrometer-tracing-bridge-otel` dependency (spec Decision Log #7).
- Versions below were pinned against Maven Central / spring.io on 2026-08-29. If Maven reports any of them unavailable at execution time, bump to the nearest available patch/version in the same line and note the substitution in that task's commit message — don't silently guess a different major version.

---

## Task 1: Project scaffolding

**Files:**
- Create: `pom.xml`
- Create: `src/main/resources/application.yml`
- Create: `src/main/java/com/template/batchconsumer/Application.java`
- Test: `src/test/java/com/template/batchconsumer/ApplicationContextTest.java`

**Interfaces:**
- Consumes: nothing (first task).
- Produces: a buildable Maven project on Spring Boot 4.1.1 with every dependency later tasks need already declared; base package `com.template.batchconsumer` exists and compiles.

- [ ] **Step 1: Write `pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>4.1.1</version>
        <relativePath/>
    </parent>

    <groupId>com.template</groupId>
    <artifactId>batch-consumer-reactive</artifactId>
    <version>0.1.0-SNAPSHOT</version>
    <name>template-spring-batch-consumer-reactive</name>
    <description>Hexagonal reactive Kafka consumer template</description>

    <properties>
        <java.version>21</java.version>
        <archunit.version>1.5.0</archunit.version>
        <testcontainers.version>2.0.5</testcontainers.version>
        <blockhound.version>1.0.13.RELEASE</blockhound.version>
        <opentelemetry.version>1.64.0</opentelemetry.version>
        <opentelemetry-instrumentation.version>2.28.1-alpha</opentelemetry-instrumentation.version>
    </properties>

    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>org.testcontainers</groupId>
                <artifactId>testcontainers-bom</artifactId>
                <version>${testcontainers.version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
            <dependency>
                <groupId>io.opentelemetry</groupId>
                <artifactId>opentelemetry-bom</artifactId>
                <version>${opentelemetry.version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
            <dependency>
                <groupId>io.opentelemetry.instrumentation</groupId>
                <artifactId>opentelemetry-instrumentation-bom-alpha</artifactId>
                <version>${opentelemetry-instrumentation.version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
        </dependencies>
    </dependencyManagement>

    <dependencies>
        <!-- Main -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-webflux</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
        </dependency>
        <dependency>
            <groupId>io.micrometer</groupId>
            <artifactId>micrometer-registry-prometheus</artifactId>
        </dependency>

        <!-- Test -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>io.projectreactor</groupId>
            <artifactId>reactor-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>com.tngtech.archunit</groupId>
            <artifactId>archunit-junit5</artifactId>
            <version>${archunit.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>kafka</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>io.projectreactor.tools</groupId>
            <artifactId>blockhound-junit-platform</artifactId>
            <version>${blockhound.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>io.opentelemetry</groupId>
            <artifactId>opentelemetry-sdk-testing</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>io.opentelemetry.instrumentation</groupId>
            <artifactId>opentelemetry-reactor-3.1</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <configuration>
                    <!-- Required for BlockHound (Task 11) to instrument the JVM on modern JDKs -->
                    <argLine>
                        -XX:+AllowRedefinitionToAddDeleteMethods
                        --add-opens java.base/jdk.internal.misc=ALL-UNNAMED
                        --add-opens java.base/java.lang=ALL-UNNAMED
                    </argLine>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 2: Write `application.yml`**

```yaml
spring:
  application:
    name: template-batch-consumer-reactive
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}

consumer:
  sample:
    id: sample-consumer
    topic: sample-events
    concurrency: 3
    retry:
      max-attempts: 3
      initial-interval-ms: 1000
      multiplier: 2.0
      max-interval-ms: 10000

management:
  endpoints:
    web:
      exposure:
        include: health,prometheus,consumers
  endpoint:
    health:
      show-details: always
```

- [ ] **Step 3: Write `Application.java`**

```java
package com.template.batchconsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
```

- [ ] **Step 4: Verify the project compiles**

Run: `mvn -q compile`
Expected: `BUILD SUCCESS`

- [ ] **Step 5: Write the smoke test**

```java
package com.template.batchconsumer;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class ApplicationContextTest {

    @Test
    void contextLoads() {
    }
}
```

- [ ] **Step 6: Run the smoke test**

Run: `mvn -q test -Dtest=ApplicationContextTest`
Expected: `BUILD SUCCESS`, 1 test run, 0 failures

- [ ] **Step 7: Commit**

```bash
git add pom.xml src/main/resources/application.yml src/main/java/com/template/batchconsumer/Application.java src/test/java/com/template/batchconsumer/ApplicationContextTest.java
git commit -m "chore: scaffold Spring Boot 4.1.1 project"
```

---

## Task 2: Domain model and exceptions

**Files:**
- Create: `src/main/java/com/template/batchconsumer/domain/model/MessageEnvelope.java`
- Create: `src/main/java/com/template/batchconsumer/domain/model/ProcessingOutcome.java`
- Create: `src/main/java/com/template/batchconsumer/domain/model/ConsumerStatus.java`
- Create: `src/main/java/com/template/batchconsumer/domain/exception/RetryableProcessingException.java`
- Create: `src/main/java/com/template/batchconsumer/domain/exception/NonRetryableProcessingException.java`
- Test: `src/test/java/com/template/batchconsumer/domain/model/MessageEnvelopeTest.java`
- Test: `src/test/java/com/template/batchconsumer/domain/model/ConsumerStatusTest.java`

**Interfaces:**
- Consumes: nothing beyond Task 1's project.
- Produces: `MessageEnvelope<T>(consumerId, topic, partition, offset, key, payload)`, `ProcessingOutcome` enum {`SUCCESS`, `RETRYABLE_FAILURE`, `NON_RETRYABLE_FAILURE`}, `ConsumerStatus(consumerId, state, asOf)` with nested enum `ConsumerStatus.State` {`RUNNING`, `PAUSED`, `STOPPED`}, `RetryableProcessingException(String[, Throwable])`, `NonRetryableProcessingException(String[, Throwable])` — every later task's domain/application/adapter code imports these exact types.

- [ ] **Step 1: Write the failing test for `MessageEnvelope`**

```java
package com.template.batchconsumer.domain.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MessageEnvelopeTest {

    @Test
    void exposesAllFieldsPassedToConstructor() {
        MessageEnvelope<String> envelope = new MessageEnvelope<>(
                "sample-consumer", "sample-events", 2, 42L, "order-1", "payload-body");

        assertThat(envelope.consumerId()).isEqualTo("sample-consumer");
        assertThat(envelope.topic()).isEqualTo("sample-events");
        assertThat(envelope.partition()).isEqualTo(2);
        assertThat(envelope.offset()).isEqualTo(42L);
        assertThat(envelope.key()).isEqualTo("order-1");
        assertThat(envelope.payload()).isEqualTo("payload-body");
    }

    @Test
    void twoEnvelopesWithSameFieldsAreEqual() {
        MessageEnvelope<String> first = new MessageEnvelope<>("c1", "t1", 0, 1L, "k1", "p1");
        MessageEnvelope<String> second = new MessageEnvelope<>("c1", "t1", 0, 1L, "k1", "p1");

        assertThat(first).isEqualTo(second);
        assertThat(first.hashCode()).isEqualTo(second.hashCode());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=MessageEnvelopeTest`
Expected: FAIL — `cannot find symbol: class MessageEnvelope`

- [ ] **Step 3: Implement `MessageEnvelope`**

```java
package com.template.batchconsumer.domain.model;

public record MessageEnvelope<T>(
        String consumerId,
        String topic,
        int partition,
        long offset,
        String key,
        T payload) {
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q test -Dtest=MessageEnvelopeTest`
Expected: PASS

- [ ] **Step 5: Write `ProcessingOutcome`**

```java
package com.template.batchconsumer.domain.model;

public enum ProcessingOutcome {
    SUCCESS,
    RETRYABLE_FAILURE,
    NON_RETRYABLE_FAILURE
}
```

- [ ] **Step 6: Write the failing test for `ConsumerStatus`**

```java
package com.template.batchconsumer.domain.model;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class ConsumerStatusTest {

    @Test
    void exposesAllFieldsPassedToConstructor() {
        Instant now = Instant.now();
        ConsumerStatus status = new ConsumerStatus("sample-consumer", ConsumerStatus.State.RUNNING, now);

        assertThat(status.consumerId()).isEqualTo("sample-consumer");
        assertThat(status.state()).isEqualTo(ConsumerStatus.State.RUNNING);
        assertThat(status.asOf()).isEqualTo(now);
    }
}
```

- [ ] **Step 7: Run test to verify it fails**

Run: `mvn -q test -Dtest=ConsumerStatusTest`
Expected: FAIL — `cannot find symbol: class ConsumerStatus`

- [ ] **Step 8: Implement `ConsumerStatus`**

```java
package com.template.batchconsumer.domain.model;

import java.time.Instant;

public record ConsumerStatus(String consumerId, State state, Instant asOf) {

    public enum State {
        RUNNING,
        PAUSED,
        STOPPED
    }
}
```

- [ ] **Step 9: Run test to verify it passes**

Run: `mvn -q test -Dtest=ConsumerStatusTest`
Expected: PASS

- [ ] **Step 10: Write the exception types (no test — pure data classes, exercised by Tasks 5 and 6)**

```java
package com.template.batchconsumer.domain.exception;

public class RetryableProcessingException extends RuntimeException {

    public RetryableProcessingException(String message) {
        super(message);
    }

    public RetryableProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

```java
package com.template.batchconsumer.domain.exception;

public class NonRetryableProcessingException extends RuntimeException {

    public NonRetryableProcessingException(String message) {
        super(message);
    }

    public NonRetryableProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

- [ ] **Step 11: Run the full test suite**

Run: `mvn -q test`
Expected: `BUILD SUCCESS`

- [ ] **Step 12: Commit**

```bash
git add src/main/java/com/template/batchconsumer/domain src/test/java/com/template/batchconsumer/domain
git commit -m "feat: add domain model and processing exceptions"
```

---

## Task 3: ArchUnit architecture governance test

**Files:**
- Test: `src/test/java/com/template/batchconsumer/architecture/HexagonalArchitectureTest.java`

**Interfaces:**
- Consumes: the package structure `com.template.batchconsumer.{domain,application,adapter.in,adapter.out,config}` (some empty until later tasks — that's fine, ArchUnit checks whatever classes exist).
- Produces: a standing test (`HexagonalArchitectureTest`) that every later task's code must keep satisfying. No new production types.

This test governs, rather than follows, TDD — it's written once and must keep passing as later tasks add classes. It passes trivially now (only `domain` has classes).

- [ ] **Step 1: Write the architecture test**

```java
package com.template.batchconsumer.architecture;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.Architectures.layeredArchitecture;

class HexagonalArchitectureTest {

    private static final String BASE_PACKAGE = "com.template.batchconsumer";

    private final JavaClasses classes = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages(BASE_PACKAGE);

    @Test
    void hexagonalLayersRespectDependencyDirection() {
        ArchRule rule = layeredArchitecture()
                .consideringOnlyDependenciesInLayers()
                .layer("Domain").definedBy(BASE_PACKAGE + ".domain..")
                .layer("Application").definedBy(BASE_PACKAGE + ".application..")
                .layer("AdapterIn").definedBy(BASE_PACKAGE + ".adapter.in..")
                .layer("AdapterOut").definedBy(BASE_PACKAGE + ".adapter.out..")
                .layer("Config").definedBy(BASE_PACKAGE + ".config..")

                .whereLayer("Domain").mayOnlyBeAccessedByLayers("Application", "AdapterIn", "AdapterOut", "Config")
                .whereLayer("Application").mayOnlyBeAccessedByLayers("AdapterIn", "AdapterOut", "Config")
                .whereLayer("AdapterIn").mayOnlyBeAccessedByLayers("Config")
                .whereLayer("AdapterOut").mayOnlyBeAccessedByLayers("Config")
                .whereLayer("Config").mayNotBeAccessedByAnyLayer();

        rule.check(classes);
    }

    @Test
    void applicationDependsOnlyOnDomainAndObservabilityFacades() {
        ArchRule rule = noClasses()
                .that().resideInAPackage(BASE_PACKAGE + ".application..")
                .should().dependOnClassesThat()
                .resideInAnyPackage("org.springframework..", "org.apache.kafka..", "io.r2dbc..", "reactor.kafka..")
                .because("application must stay framework-agnostic except for cross-cutting "
                        + "observability (Micrometer/SLF4J) — Spring, the Kafka client, R2DBC, "
                        + "and Reactor Kafka are explicitly kept out");

        rule.check(classes);
    }

    @Test
    void adapterInDoesNotDependOnAdapterOutDirectly() {
        ArchRule rule = noClasses()
                .that().resideInAPackage(BASE_PACKAGE + ".adapter.in..")
                .should().dependOnClassesThat()
                .resideInAPackage(BASE_PACKAGE + ".adapter.out..")
                .because("adapter.in must reach adapter.out functionality only through "
                        + "application.port.out interfaces");

        rule.check(classes);
    }
}
```

- [ ] **Step 2: Run the test — it must pass against the current (domain-only) codebase**

Run: `mvn -q test -Dtest=HexagonalArchitectureTest`
Expected: PASS (no classes yet in `application`/`adapter`/`config`, so nothing to violate)

- [ ] **Step 3: Commit**

```bash
git add src/test/java/com/template/batchconsumer/architecture/HexagonalArchitectureTest.java
git commit -m "test: add ArchUnit hexagonal architecture governance rules"
```

---

## Task 4: Application ports

**Files:**
- Create: `src/main/java/com/template/batchconsumer/application/port/in/MessageProcessor.java`
- Create: `src/main/java/com/template/batchconsumer/application/port/in/ConsumeMessageUseCase.java`
- Create: `src/main/java/com/template/batchconsumer/application/port/in/ConsumerLifecycleUseCase.java`
- Create: `src/main/java/com/template/batchconsumer/application/port/out/ConsumerLifecycleControlPort.java`

**Interfaces:**
- Consumes: `MessageEnvelope<T>`, `ProcessingOutcome`, `ConsumerStatus` (Task 2).
- Produces: `MessageProcessor<T>.process(MessageEnvelope<T>): Mono<ProcessingOutcome>`; `ConsumeMessageUseCase<T>.consume(MessageEnvelope<T>): Mono<ProcessingOutcome>`; `ConsumerLifecycleUseCase` and `ConsumerLifecycleControlPort`, both with identical signatures `start(String)`, `stop(String)`, `pause(String)`, `resume(String)`, `status(String): ConsumerStatus`, `statuses(): List<ConsumerStatus>` — Task 5 implements both use-case interfaces, Task 7 implements the control port, Task 8/9 depend on the use-case interfaces.

These are interfaces only — no behavior to TDD. Steps verify compilation and that the architecture rules still hold.

- [ ] **Step 1: Write `MessageProcessor`**

```java
package com.template.batchconsumer.application.port.in;

import com.template.batchconsumer.domain.model.MessageEnvelope;
import com.template.batchconsumer.domain.model.ProcessingOutcome;
import reactor.core.publisher.Mono;

public interface MessageProcessor<T> {

    Mono<ProcessingOutcome> process(MessageEnvelope<T> envelope);
}
```

- [ ] **Step 2: Write `ConsumeMessageUseCase`**

```java
package com.template.batchconsumer.application.port.in;

import com.template.batchconsumer.domain.model.MessageEnvelope;
import com.template.batchconsumer.domain.model.ProcessingOutcome;
import reactor.core.publisher.Mono;

public interface ConsumeMessageUseCase<T> {

    Mono<ProcessingOutcome> consume(MessageEnvelope<T> envelope);
}
```

- [ ] **Step 3: Write `ConsumerLifecycleUseCase`**

```java
package com.template.batchconsumer.application.port.in;

import com.template.batchconsumer.domain.model.ConsumerStatus;

import java.util.List;

public interface ConsumerLifecycleUseCase {

    void start(String consumerId);

    void stop(String consumerId);

    void pause(String consumerId);

    void resume(String consumerId);

    ConsumerStatus status(String consumerId);

    List<ConsumerStatus> statuses();
}
```

- [ ] **Step 4: Write `ConsumerLifecycleControlPort`**

```java
package com.template.batchconsumer.application.port.out;

import com.template.batchconsumer.domain.model.ConsumerStatus;

import java.util.List;

public interface ConsumerLifecycleControlPort {

    void start(String consumerId);

    void stop(String consumerId);

    void pause(String consumerId);

    void resume(String consumerId);

    ConsumerStatus status(String consumerId);

    List<ConsumerStatus> statuses();
}
```

- [ ] **Step 5: Verify the project compiles and the architecture test still passes**

Run: `mvn -q test -Dtest=HexagonalArchitectureTest`
Expected: PASS (interfaces in `application.port.in`/`application.port.out` depend only on `domain` and `reactor.core.publisher`, which is allowed)

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/template/batchconsumer/application/port
git commit -m "feat: add application port interfaces"
```

---

## Task 5: ConsumerOrchestrationService

**Files:**
- Create: `src/main/java/com/template/batchconsumer/application/service/ConsumerOrchestrationService.java`
- Test: `src/test/java/com/template/batchconsumer/application/service/ConsumerOrchestrationServiceTest.java`

**Interfaces:**
- Consumes: `MessageProcessor<T>`, `ConsumeMessageUseCase<T>`, `ConsumerLifecycleUseCase`, `ConsumerLifecycleControlPort` (Task 4); `MessageEnvelope<T>`, `ProcessingOutcome`, `ConsumerStatus`, `RetryableProcessingException`, `NonRetryableProcessingException` (Task 2).
- Produces: `ConsumerOrchestrationService<T>(String consumerId, MessageProcessor<T> processor, MeterRegistry meterRegistry, ConsumerLifecycleControlPort port)` implementing both `ConsumeMessageUseCase<T>` and `ConsumerLifecycleUseCase` — Task 10 wires this as a Spring bean.

Metrics recorded here: `consumer.messages.processed{consumer,outcome}` (counter, one increment per terminal outcome) and `consumer.processing.duration{consumer}` (timer). `consumer.messages.retried` / `consumer.messages.dlt` are **not** recorded here — they're recorded in Task 10's Kafka error-handler config, since retries/DLT happen outside this service (Spring Kafka re-invokes the whole listener method on each retry).

- [ ] **Step 1: Write the failing test for the success path**

```java
package com.template.batchconsumer.application.service;

import com.template.batchconsumer.application.port.in.MessageProcessor;
import com.template.batchconsumer.application.port.out.ConsumerLifecycleControlPort;
import com.template.batchconsumer.domain.model.MessageEnvelope;
import com.template.batchconsumer.domain.model.ProcessingOutcome;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

class ConsumerOrchestrationServiceTest {

    private MessageProcessor<String> messageProcessor;
    private ConsumerLifecycleControlPort lifecycleControlPort;
    private SimpleMeterRegistry meterRegistry;
    private ConsumerOrchestrationService<String> service;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        messageProcessor = Mockito.mock(MessageProcessor.class);
        lifecycleControlPort = Mockito.mock(ConsumerLifecycleControlPort.class);
        meterRegistry = new SimpleMeterRegistry();
        service = new ConsumerOrchestrationService<>("sample-consumer", messageProcessor, meterRegistry, lifecycleControlPort);
    }

    @Test
    void consumeRecordsSuccessOutcomeAndDuration() {
        MessageEnvelope<String> envelope = new MessageEnvelope<>("sample-consumer", "sample-events", 0, 1L, "k1", "payload");
        when(messageProcessor.process(envelope)).thenReturn(Mono.just(ProcessingOutcome.SUCCESS));

        StepVerifier.create(service.consume(envelope))
                .expectNext(ProcessingOutcome.SUCCESS)
                .verifyComplete();

        assertThat(meterRegistry.counter("consumer.messages.processed", "consumer", "sample-consumer", "outcome", "SUCCESS").count())
                .isEqualTo(1.0);
        assertThat(meterRegistry.timer("consumer.processing.duration", "consumer", "sample-consumer").count())
                .isEqualTo(1);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=ConsumerOrchestrationServiceTest`
Expected: FAIL — `cannot find symbol: class ConsumerOrchestrationService`

- [ ] **Step 3: Implement `ConsumerOrchestrationService`**

```java
package com.template.batchconsumer.application.service;

import com.template.batchconsumer.application.port.in.ConsumeMessageUseCase;
import com.template.batchconsumer.application.port.in.ConsumerLifecycleUseCase;
import com.template.batchconsumer.application.port.in.MessageProcessor;
import com.template.batchconsumer.application.port.out.ConsumerLifecycleControlPort;
import com.template.batchconsumer.domain.exception.NonRetryableProcessingException;
import com.template.batchconsumer.domain.model.ConsumerStatus;
import com.template.batchconsumer.domain.model.MessageEnvelope;
import com.template.batchconsumer.domain.model.ProcessingOutcome;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import reactor.core.publisher.Mono;

import java.util.List;

public class ConsumerOrchestrationService<T> implements ConsumeMessageUseCase<T>, ConsumerLifecycleUseCase {

    private final String consumerId;
    private final MessageProcessor<T> messageProcessor;
    private final MeterRegistry meterRegistry;
    private final ConsumerLifecycleControlPort lifecycleControlPort;

    public ConsumerOrchestrationService(
            String consumerId,
            MessageProcessor<T> messageProcessor,
            MeterRegistry meterRegistry,
            ConsumerLifecycleControlPort lifecycleControlPort) {
        this.consumerId = consumerId;
        this.messageProcessor = messageProcessor;
        this.meterRegistry = meterRegistry;
        this.lifecycleControlPort = lifecycleControlPort;
    }

    @Override
    public Mono<ProcessingOutcome> consume(MessageEnvelope<T> envelope) {
        Timer.Sample sample = Timer.start(meterRegistry);
        return messageProcessor.process(envelope)
                .doOnNext(outcome -> recordSuccessOutcome(outcome, sample))
                .doOnError(error -> recordErrorOutcome(error, sample));
    }

    private void recordSuccessOutcome(ProcessingOutcome outcome, Timer.Sample sample) {
        meterRegistry.counter("consumer.messages.processed", "consumer", consumerId, "outcome", outcome.name())
                .increment();
        sample.stop(meterRegistry.timer("consumer.processing.duration", "consumer", consumerId));
    }

    private void recordErrorOutcome(Throwable error, Timer.Sample sample) {
        ProcessingOutcome outcome = error instanceof NonRetryableProcessingException
                ? ProcessingOutcome.NON_RETRYABLE_FAILURE
                : ProcessingOutcome.RETRYABLE_FAILURE;
        meterRegistry.counter("consumer.messages.processed", "consumer", consumerId, "outcome", outcome.name())
                .increment();
        sample.stop(meterRegistry.timer("consumer.processing.duration", "consumer", consumerId));
    }

    @Override
    public void start(String targetConsumerId) {
        lifecycleControlPort.start(targetConsumerId);
    }

    @Override
    public void stop(String targetConsumerId) {
        lifecycleControlPort.stop(targetConsumerId);
    }

    @Override
    public void pause(String targetConsumerId) {
        lifecycleControlPort.pause(targetConsumerId);
    }

    @Override
    public void resume(String targetConsumerId) {
        lifecycleControlPort.resume(targetConsumerId);
    }

    @Override
    public ConsumerStatus status(String targetConsumerId) {
        return lifecycleControlPort.status(targetConsumerId);
    }

    @Override
    public List<ConsumerStatus> statuses() {
        return lifecycleControlPort.statuses();
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q test -Dtest=ConsumerOrchestrationServiceTest`
Expected: PASS

- [ ] **Step 5: Add the retryable/non-retryable/lifecycle test cases**

Append to `ConsumerOrchestrationServiceTest`:

```java
    @Test
    void consumeRecordsRetryableFailureOutcomeAndPropagatesError() {
        MessageEnvelope<String> envelope = new MessageEnvelope<>("sample-consumer", "sample-events", 0, 2L, "k2", "payload");
        com.template.batchconsumer.domain.exception.RetryableProcessingException failure =
                new com.template.batchconsumer.domain.exception.RetryableProcessingException("transient");
        when(messageProcessor.process(envelope)).thenReturn(Mono.error(failure));

        StepVerifier.create(service.consume(envelope))
                .expectErrorMatches(error -> error == failure)
                .verify();

        assertThat(meterRegistry.counter("consumer.messages.processed", "consumer", "sample-consumer", "outcome", "RETRYABLE_FAILURE").count())
                .isEqualTo(1.0);
    }

    @Test
    void consumeRecordsNonRetryableFailureOutcomeAndPropagatesError() {
        MessageEnvelope<String> envelope = new MessageEnvelope<>("sample-consumer", "sample-events", 0, 3L, "k3", "payload");
        com.template.batchconsumer.domain.exception.NonRetryableProcessingException failure =
                new com.template.batchconsumer.domain.exception.NonRetryableProcessingException("fatal");
        when(messageProcessor.process(envelope)).thenReturn(Mono.error(failure));

        StepVerifier.create(service.consume(envelope))
                .expectErrorMatches(error -> error == failure)
                .verify();

        assertThat(meterRegistry.counter("consumer.messages.processed", "consumer", "sample-consumer", "outcome", "NON_RETRYABLE_FAILURE").count())
                .isEqualTo(1.0);
    }

    @Test
    void lifecycleMethodsDelegateToControlPort() {
        service.start("sample-consumer");
        service.pause("sample-consumer");
        service.resume("sample-consumer");
        service.stop("sample-consumer");

        com.template.batchconsumer.domain.model.ConsumerStatus expectedStatus =
                new com.template.batchconsumer.domain.model.ConsumerStatus(
                        "sample-consumer", com.template.batchconsumer.domain.model.ConsumerStatus.State.RUNNING, java.time.Instant.now());
        when(lifecycleControlPort.status("sample-consumer")).thenReturn(expectedStatus);
        when(lifecycleControlPort.statuses()).thenReturn(java.util.List.of(expectedStatus));

        assertThat(service.status("sample-consumer")).isEqualTo(expectedStatus);
        assertThat(service.statuses()).containsExactly(expectedStatus);

        org.mockito.Mockito.verify(lifecycleControlPort).start("sample-consumer");
        org.mockito.Mockito.verify(lifecycleControlPort).pause("sample-consumer");
        org.mockito.Mockito.verify(lifecycleControlPort).resume("sample-consumer");
        org.mockito.Mockito.verify(lifecycleControlPort).stop("sample-consumer");
    }
```

- [ ] **Step 6: Run the full test class**

Run: `mvn -q test -Dtest=ConsumerOrchestrationServiceTest`
Expected: PASS, 4 tests

- [ ] **Step 7: Run the architecture test to confirm no boundary violation**

Run: `mvn -q test -Dtest=HexagonalArchitectureTest`
Expected: PASS (`ConsumerOrchestrationService` imports only `domain` and `io.micrometer..`, both allowed)

- [ ] **Step 8: Commit**

```bash
git add src/main/java/com/template/batchconsumer/application/service src/test/java/com/template/batchconsumer/application/service
git commit -m "feat: add ConsumerOrchestrationService"
```

---

## Task 6: Sample business slice (SamplePayload + SampleMessageProcessor)

**Files:**
- Create: `src/main/java/com/template/batchconsumer/application/sample/SamplePayload.java`
- Create: `src/main/java/com/template/batchconsumer/application/sample/SampleMessageProcessor.java`
- Test: `src/test/java/com/template/batchconsumer/application/sample/SampleMessageProcessorTest.java`

**Interfaces:**
- Consumes: `MessageProcessor<T>` (Task 4), `MessageEnvelope<T>`, `ProcessingOutcome`, `RetryableProcessingException`, `NonRetryableProcessingException` (Task 2).
- Produces: `SamplePayload(String id, String message)`; `SampleMessageProcessor implements MessageProcessor<SamplePayload>` with two magic trigger strings — `SampleMessageProcessor.RETRYABLE_TRIGGER` ("FAIL_RETRYABLE") and `SampleMessageProcessor.NON_RETRYABLE_TRIGGER` ("FAIL_FATAL") — any other `message` value succeeds. Task 8's Kafka adapter and Task 10's config and Task 12's integration tests all depend on this exact contract. **This whole slice is example-only — delete it when adopting the template for a real consumer.**

- [ ] **Step 1: Write `SamplePayload`**

```java
package com.template.batchconsumer.application.sample;

public record SamplePayload(String id, String message) {
}
```

- [ ] **Step 2: Write the failing test for the success path**

```java
package com.template.batchconsumer.application.sample;

import com.template.batchconsumer.domain.model.MessageEnvelope;
import com.template.batchconsumer.domain.model.ProcessingOutcome;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

class SampleMessageProcessorTest {

    private final SampleMessageProcessor processor = new SampleMessageProcessor();

    @Test
    void succeedsForOrdinaryPayload() {
        MessageEnvelope<SamplePayload> envelope = envelopeWithMessage("hello world");

        StepVerifier.create(processor.process(envelope))
                .expectNext(ProcessingOutcome.SUCCESS)
                .verifyComplete();
    }

    private MessageEnvelope<SamplePayload> envelopeWithMessage(String message) {
        return new MessageEnvelope<>("sample-consumer", "sample-events", 0, 0L, "key", new SamplePayload("id-1", message));
    }
}
```

- [ ] **Step 2b: Run test to verify it fails**

Run: `mvn -q test -Dtest=SampleMessageProcessorTest`
Expected: FAIL — `cannot find symbol: class SampleMessageProcessor`

- [ ] **Step 3: Implement `SampleMessageProcessor`**

```java
package com.template.batchconsumer.application.sample;

import com.template.batchconsumer.application.port.in.MessageProcessor;
import com.template.batchconsumer.domain.exception.NonRetryableProcessingException;
import com.template.batchconsumer.domain.exception.RetryableProcessingException;
import com.template.batchconsumer.domain.model.MessageEnvelope;
import com.template.batchconsumer.domain.model.ProcessingOutcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * Example implementation of {@link MessageProcessor} — proves the wiring end-to-end.
 * Delete this class (and its listener wiring in adapter.in.kafka) when adopting the
 * template for a real consumer.
 */
public class SampleMessageProcessor implements MessageProcessor<SamplePayload> {

    public static final String RETRYABLE_TRIGGER = "FAIL_RETRYABLE";
    public static final String NON_RETRYABLE_TRIGGER = "FAIL_FATAL";

    private static final Logger log = LoggerFactory.getLogger(SampleMessageProcessor.class);

    @Override
    public Mono<ProcessingOutcome> process(MessageEnvelope<SamplePayload> envelope) {
        String message = envelope.payload().message();
        if (RETRYABLE_TRIGGER.equals(message)) {
            return Mono.error(new RetryableProcessingException(
                    "simulated transient failure for id=" + envelope.payload().id()));
        }
        if (NON_RETRYABLE_TRIGGER.equals(message)) {
            return Mono.error(new NonRetryableProcessingException(
                    "simulated fatal failure for id=" + envelope.payload().id()));
        }
        log.info("Processed sample message id={} message={}", envelope.payload().id(), message);
        return Mono.just(ProcessingOutcome.SUCCESS);
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q test -Dtest=SampleMessageProcessorTest`
Expected: PASS

- [ ] **Step 5: Add the retryable and non-retryable trigger tests**

Append to `SampleMessageProcessorTest`:

```java
    @Test
    void raisesRetryableExceptionForRetryTrigger() {
        MessageEnvelope<SamplePayload> envelope = envelopeWithMessage(SampleMessageProcessor.RETRYABLE_TRIGGER);

        StepVerifier.create(processor.process(envelope))
                .expectError(com.template.batchconsumer.domain.exception.RetryableProcessingException.class)
                .verify();
    }

    @Test
    void raisesNonRetryableExceptionForFatalTrigger() {
        MessageEnvelope<SamplePayload> envelope = envelopeWithMessage(SampleMessageProcessor.NON_RETRYABLE_TRIGGER);

        StepVerifier.create(processor.process(envelope))
                .expectError(com.template.batchconsumer.domain.exception.NonRetryableProcessingException.class)
                .verify();
    }
```

- [ ] **Step 6: Run the full test class**

Run: `mvn -q test -Dtest=SampleMessageProcessorTest`
Expected: PASS, 3 tests

- [ ] **Step 7: Run the architecture test**

Run: `mvn -q test -Dtest=HexagonalArchitectureTest`
Expected: PASS (`application.sample` is under `application..`, imports only `domain`, `org.slf4j`, and `reactor.core.publisher` — all allowed)

- [ ] **Step 8: Commit**

```bash
git add src/main/java/com/template/batchconsumer/application/sample src/test/java/com/template/batchconsumer/application/sample
git commit -m "feat: add disposable SamplePayload/SampleMessageProcessor example"
```

---

## Task 7: KafkaListenerLifecycleAdapter

**Files:**
- Create: `src/main/java/com/template/batchconsumer/adapter/out/kafka/KafkaListenerLifecycleAdapter.java`
- Test: `src/test/java/com/template/batchconsumer/adapter/out/kafka/KafkaListenerLifecycleAdapterTest.java`

**Interfaces:**
- Consumes: `ConsumerLifecycleControlPort` (Task 4), `ConsumerStatus` (Task 2), Spring Kafka's `KafkaListenerEndpointRegistry` / `MessageListenerContainer`.
- Produces: `KafkaListenerLifecycleAdapter(KafkaListenerEndpointRegistry registry) implements ConsumerLifecycleControlPort` — Task 10 wires this as the `ConsumerLifecycleControlPort` bean.

- [ ] **Step 1: Write the failing test for `start`**

```java
package com.template.batchconsumer.adapter.out.kafka;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaListenerLifecycleAdapterTest {

    private KafkaListenerEndpointRegistry registry;
    private MessageListenerContainer container;
    private KafkaListenerLifecycleAdapter adapter;

    @BeforeEach
    void setUp() {
        registry = Mockito.mock(KafkaListenerEndpointRegistry.class);
        container = Mockito.mock(MessageListenerContainer.class);
        when(registry.getListenerContainer("sample-consumer")).thenReturn(container);
        adapter = new KafkaListenerLifecycleAdapter(registry);
    }

    @Test
    void startDelegatesToContainer() {
        adapter.start("sample-consumer");
        verify(container).start();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=KafkaListenerLifecycleAdapterTest`
Expected: FAIL — `cannot find symbol: class KafkaListenerLifecycleAdapter`

- [ ] **Step 3: Implement `KafkaListenerLifecycleAdapter`**

```java
package com.template.batchconsumer.adapter.out.kafka;

import com.template.batchconsumer.application.port.out.ConsumerLifecycleControlPort;
import com.template.batchconsumer.domain.model.ConsumerStatus;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.time.Instant;
import java.util.List;
import java.util.NoSuchElementException;

public class KafkaListenerLifecycleAdapter implements ConsumerLifecycleControlPort {

    private final KafkaListenerEndpointRegistry registry;

    public KafkaListenerLifecycleAdapter(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void start(String consumerId) {
        containerFor(consumerId).start();
    }

    @Override
    public void stop(String consumerId) {
        containerFor(consumerId).stop();
    }

    @Override
    public void pause(String consumerId) {
        containerFor(consumerId).pause();
    }

    @Override
    public void resume(String consumerId) {
        containerFor(consumerId).resume();
    }

    @Override
    public ConsumerStatus status(String consumerId) {
        return toStatus(consumerId, containerFor(consumerId));
    }

    @Override
    public List<ConsumerStatus> statuses() {
        return registry.getListenerContainerIds().stream()
                .map(id -> toStatus(id, registry.getListenerContainer(id)))
                .toList();
    }

    private MessageListenerContainer containerFor(String consumerId) {
        MessageListenerContainer container = registry.getListenerContainer(consumerId);
        if (container == null) {
            throw new NoSuchElementException("No Kafka listener container registered with id " + consumerId);
        }
        return container;
    }

    private ConsumerStatus toStatus(String consumerId, MessageListenerContainer container) {
        ConsumerStatus.State state;
        if (!container.isRunning()) {
            state = ConsumerStatus.State.STOPPED;
        } else if (container.isContainerPaused()) {
            state = ConsumerStatus.State.PAUSED;
        } else {
            state = ConsumerStatus.State.RUNNING;
        }
        return new ConsumerStatus(consumerId, state, Instant.now());
    }
}
```

*(If `isContainerPaused()` doesn't compile against the resolved Spring Kafka version, check the current `MessageListenerContainer` Javadoc for its exact pause-state accessor name and adjust.)*

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q test -Dtest=KafkaListenerLifecycleAdapterTest`
Expected: PASS

- [ ] **Step 5: Add the remaining lifecycle and status tests**

Append to `KafkaListenerLifecycleAdapterTest`:

```java
    @Test
    void stopDelegatesToContainer() {
        adapter.stop("sample-consumer");
        verify(container).stop();
    }

    @Test
    void pauseDelegatesToContainer() {
        adapter.pause("sample-consumer");
        verify(container).pause();
    }

    @Test
    void resumeDelegatesToContainer() {
        adapter.resume("sample-consumer");
        verify(container).resume();
    }

    @Test
    void statusReflectsRunningContainer() {
        when(container.isRunning()).thenReturn(true);
        when(container.isContainerPaused()).thenReturn(false);

        var status = adapter.status("sample-consumer");

        org.assertj.core.api.Assertions.assertThat(status.consumerId()).isEqualTo("sample-consumer");
        org.assertj.core.api.Assertions.assertThat(status.state())
                .isEqualTo(com.template.batchconsumer.domain.model.ConsumerStatus.State.RUNNING);
    }

    @Test
    void statusReflectsPausedContainer() {
        when(container.isRunning()).thenReturn(true);
        when(container.isContainerPaused()).thenReturn(true);

        org.assertj.core.api.Assertions.assertThat(adapter.status("sample-consumer").state())
                .isEqualTo(com.template.batchconsumer.domain.model.ConsumerStatus.State.PAUSED);
    }

    @Test
    void statusReflectsStoppedContainer() {
        when(container.isRunning()).thenReturn(false);

        org.assertj.core.api.Assertions.assertThat(adapter.status("sample-consumer").state())
                .isEqualTo(com.template.batchconsumer.domain.model.ConsumerStatus.State.STOPPED);
    }

    @Test
    void statusThrowsForUnknownConsumerId() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> adapter.status("missing-consumer"))
                .isInstanceOf(java.util.NoSuchElementException.class);
    }

    @Test
    void statusesListsAllRegisteredContainers() {
        when(registry.getListenerContainerIds()).thenReturn(java.util.List.of("sample-consumer"));
        when(container.isRunning()).thenReturn(true);
        when(container.isContainerPaused()).thenReturn(false);

        var statuses = adapter.statuses();

        org.assertj.core.api.Assertions.assertThat(statuses).hasSize(1);
        org.assertj.core.api.Assertions.assertThat(statuses.get(0).consumerId()).isEqualTo("sample-consumer");
    }
```

- [ ] **Step 6: Run the full test class**

Run: `mvn -q test -Dtest=KafkaListenerLifecycleAdapterTest`
Expected: PASS, 8 tests

- [ ] **Step 7: Run the architecture test**

Run: `mvn -q test -Dtest=HexagonalArchitectureTest`
Expected: PASS (`adapter.out.kafka` is allowed to import Spring Kafka types; `application`/`domain` untouched)

- [ ] **Step 8: Commit**

```bash
git add src/main/java/com/template/batchconsumer/adapter/out/kafka src/test/java/com/template/batchconsumer/adapter/out/kafka
git commit -m "feat: add KafkaListenerLifecycleAdapter"
```

---

## Task 8: SampleKafkaListenerAdapter

**Files:**
- Create: `src/main/java/com/template/batchconsumer/adapter/in/kafka/SampleKafkaListenerAdapter.java`
- Test: `src/test/java/com/template/batchconsumer/adapter/in/kafka/SampleKafkaListenerAdapterTest.java`

**Interfaces:**
- Consumes: `ConsumeMessageUseCase<T>` (Task 4), `SamplePayload` (Task 6), `MessageEnvelope<T>` (Task 2).
- Produces: `SampleKafkaListenerAdapter(ConsumeMessageUseCase<SamplePayload>, String consumerId)`, package-visible `toEnvelope(ConsumerRecord<String, SamplePayload>): MessageEnvelope<SamplePayload>` (unit-testable without a broker), and the `@KafkaListener` method `onMessage(ConsumerRecord<String, SamplePayload>)` — references container factory bean name `sampleKafkaListenerContainerFactory` and properties `consumer.sample.id`/`consumer.sample.topic`/`consumer.sample.concurrency`, all provided by Task 10. **Example-only, like Task 6 — delete both together when adopting the template.**

- [ ] **Step 1: Write the failing test for envelope conversion**

```java
package com.template.batchconsumer.adapter.in.kafka;

import com.template.batchconsumer.application.port.in.ConsumeMessageUseCase;
import com.template.batchconsumer.application.sample.SamplePayload;
import com.template.batchconsumer.domain.model.MessageEnvelope;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

class SampleKafkaListenerAdapterTest {

    @SuppressWarnings("unchecked")
    private final ConsumeMessageUseCase<SamplePayload> consumeMessageUseCase = Mockito.mock(ConsumeMessageUseCase.class);
    private final SampleKafkaListenerAdapter adapter =
            new SampleKafkaListenerAdapter(consumeMessageUseCase, "sample-consumer");

    @Test
    void toEnvelopeCopiesAllRecordFields() {
        SamplePayload payload = new SamplePayload("id-1", "hello");
        ConsumerRecord<String, SamplePayload> record =
                new ConsumerRecord<>("sample-events", 2, 42L, "key-1", payload);

        MessageEnvelope<SamplePayload> envelope = adapter.toEnvelope(record);

        assertThat(envelope.consumerId()).isEqualTo("sample-consumer");
        assertThat(envelope.topic()).isEqualTo("sample-events");
        assertThat(envelope.partition()).isEqualTo(2);
        assertThat(envelope.offset()).isEqualTo(42L);
        assertThat(envelope.key()).isEqualTo("key-1");
        assertThat(envelope.payload()).isEqualTo(payload);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=SampleKafkaListenerAdapterTest`
Expected: FAIL — `cannot find symbol: class SampleKafkaListenerAdapter`

- [ ] **Step 3: Implement `SampleKafkaListenerAdapter`**

```java
package com.template.batchconsumer.adapter.in.kafka;

import com.template.batchconsumer.application.port.in.ConsumeMessageUseCase;
import com.template.batchconsumer.application.sample.SamplePayload;
import com.template.batchconsumer.domain.model.MessageEnvelope;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class SampleKafkaListenerAdapter {

    private static final Duration BLOCK_TIMEOUT = Duration.ofSeconds(30);

    private final ConsumeMessageUseCase<SamplePayload> consumeMessageUseCase;
    private final String consumerId;

    public SampleKafkaListenerAdapter(
            ConsumeMessageUseCase<SamplePayload> consumeMessageUseCase,
            @Value("${consumer.sample.id}") String consumerId) {
        this.consumeMessageUseCase = consumeMessageUseCase;
        this.consumerId = consumerId;
    }

    @KafkaListener(
            id = "${consumer.sample.id}",
            topics = "${consumer.sample.topic}",
            concurrency = "${consumer.sample.concurrency}",
            containerFactory = "sampleKafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, SamplePayload> record) {
        // Single, outermost blocking bridge point (spec Decision Log #3) — everything
        // consumeMessageUseCase.consume(...) does stays reactive; this is the only .block().
        consumeMessageUseCase.consume(toEnvelope(record)).block(BLOCK_TIMEOUT);
    }

    MessageEnvelope<SamplePayload> toEnvelope(ConsumerRecord<String, SamplePayload> record) {
        return new MessageEnvelope<>(
                consumerId,
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value());
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q test -Dtest=SampleKafkaListenerAdapterTest`
Expected: PASS

- [ ] **Step 5: Add the `onMessage` blocking-bridge test**

Append to `SampleKafkaListenerAdapterTest`:

```java
    @Test
    void onMessageBlocksOnComposedUseCaseMono() {
        SamplePayload payload = new SamplePayload("id-2", "hello");
        ConsumerRecord<String, SamplePayload> record =
                new ConsumerRecord<>("sample-events", 0, 0L, "key-2", payload);
        Mockito.when(consumeMessageUseCase.consume(Mockito.any()))
                .thenReturn(reactor.core.publisher.Mono.just(com.template.batchconsumer.domain.model.ProcessingOutcome.SUCCESS));

        adapter.onMessage(record);

        Mockito.verify(consumeMessageUseCase).consume(adapter.toEnvelope(record));
    }
```

- [ ] **Step 6: Run the full test class**

Run: `mvn -q test -Dtest=SampleKafkaListenerAdapterTest`
Expected: PASS, 2 tests

- [ ] **Step 7: Run the architecture test**

Run: `mvn -q test -Dtest=HexagonalArchitectureTest`
Expected: PASS (`adapter.in.kafka` depends on `application.port.in` and `domain`, not `adapter.out`)

- [ ] **Step 8: Commit**

```bash
git add src/main/java/com/template/batchconsumer/adapter/in/kafka src/test/java/com/template/batchconsumer/adapter/in/kafka
git commit -m "feat: add disposable SampleKafkaListenerAdapter"
```

---

## Task 9: ConsumerLifecycleEndpoint

**Files:**
- Create: `src/main/java/com/template/batchconsumer/adapter/in/web/ConsumerLifecycleEndpoint.java`
- Test: `src/test/java/com/template/batchconsumer/adapter/in/web/ConsumerLifecycleEndpointTest.java`

**Interfaces:**
- Consumes: `ConsumerLifecycleUseCase` (Task 4), `ConsumerStatus` (Task 2).
- Produces: `ConsumerLifecycleEndpoint(ConsumerLifecycleUseCase)`, Actuator endpoint id `consumers`, with nested `ConsumerLifecycleEndpoint.Action` enum {`PAUSE`, `RESUME`, `STOP`, `START`} — Task 10 wires this as a bean.

- [ ] **Step 1: Write the failing test for `statuses`**

```java
package com.template.batchconsumer.adapter.in.web;

import com.template.batchconsumer.application.port.in.ConsumerLifecycleUseCase;
import com.template.batchconsumer.domain.model.ConsumerStatus;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

class ConsumerLifecycleEndpointTest {

    private final ConsumerLifecycleUseCase useCase = Mockito.mock(ConsumerLifecycleUseCase.class);
    private final ConsumerLifecycleEndpoint endpoint = new ConsumerLifecycleEndpoint(useCase);

    @Test
    void statusesDelegatesToUseCase() {
        ConsumerStatus status = new ConsumerStatus("sample-consumer", ConsumerStatus.State.RUNNING, Instant.now());
        when(useCase.statuses()).thenReturn(List.of(status));

        assertThat(endpoint.statuses()).containsExactly(status);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=ConsumerLifecycleEndpointTest`
Expected: FAIL — `cannot find symbol: class ConsumerLifecycleEndpoint`

- [ ] **Step 3: Implement `ConsumerLifecycleEndpoint`**

```java
package com.template.batchconsumer.adapter.in.web;

import com.template.batchconsumer.application.port.in.ConsumerLifecycleUseCase;
import com.template.batchconsumer.domain.model.ConsumerStatus;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;

import java.util.List;

@Endpoint(id = "consumers")
public class ConsumerLifecycleEndpoint {

    private final ConsumerLifecycleUseCase consumerLifecycleUseCase;

    public ConsumerLifecycleEndpoint(ConsumerLifecycleUseCase consumerLifecycleUseCase) {
        this.consumerLifecycleUseCase = consumerLifecycleUseCase;
    }

    @ReadOperation
    public List<ConsumerStatus> statuses() {
        return consumerLifecycleUseCase.statuses();
    }

    @ReadOperation
    public ConsumerStatus status(@Selector String consumerId) {
        return consumerLifecycleUseCase.status(consumerId);
    }

    @WriteOperation
    public void applyAction(@Selector String consumerId, @Selector Action action) {
        switch (action) {
            case PAUSE -> consumerLifecycleUseCase.pause(consumerId);
            case RESUME -> consumerLifecycleUseCase.resume(consumerId);
            case STOP -> consumerLifecycleUseCase.stop(consumerId);
            case START -> consumerLifecycleUseCase.start(consumerId);
        }
    }

    public enum Action {
        PAUSE, RESUME, STOP, START
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q test -Dtest=ConsumerLifecycleEndpointTest`
Expected: PASS

- [ ] **Step 5: Add the remaining endpoint tests**

Append to `ConsumerLifecycleEndpointTest`:

```java
    @Test
    void statusDelegatesToUseCaseForGivenId() {
        ConsumerStatus status = new ConsumerStatus("sample-consumer", ConsumerStatus.State.PAUSED, Instant.now());
        when(useCase.status("sample-consumer")).thenReturn(status);

        assertThat(endpoint.status("sample-consumer")).isEqualTo(status);
    }

    @Test
    void applyActionPauseCallsPause() {
        endpoint.applyAction("sample-consumer", ConsumerLifecycleEndpoint.Action.PAUSE);
        org.mockito.Mockito.verify(useCase).pause("sample-consumer");
    }

    @Test
    void applyActionResumeCallsResume() {
        endpoint.applyAction("sample-consumer", ConsumerLifecycleEndpoint.Action.RESUME);
        org.mockito.Mockito.verify(useCase).resume("sample-consumer");
    }

    @Test
    void applyActionStopCallsStop() {
        endpoint.applyAction("sample-consumer", ConsumerLifecycleEndpoint.Action.STOP);
        org.mockito.Mockito.verify(useCase).stop("sample-consumer");
    }

    @Test
    void applyActionStartCallsStart() {
        endpoint.applyAction("sample-consumer", ConsumerLifecycleEndpoint.Action.START);
        org.mockito.Mockito.verify(useCase).start("sample-consumer");
    }
```

- [ ] **Step 6: Run the full test class**

Run: `mvn -q test -Dtest=ConsumerLifecycleEndpointTest`
Expected: PASS, 6 tests

- [ ] **Step 7: Run the architecture test**

Run: `mvn -q test -Dtest=HexagonalArchitectureTest`
Expected: PASS (`adapter.in.web` depends on `application.port.in` and `domain`, not `adapter.out`)

- [ ] **Step 8: Commit**

```bash
git add src/main/java/com/template/batchconsumer/adapter/in/web src/test/java/com/template/batchconsumer/adapter/in/web
git commit -m "feat: add ConsumerLifecycleEndpoint actuator endpoint"
```

---

## Task 10: Kafka configuration and bean wiring

**Files:**
- Create: `src/main/java/com/template/batchconsumer/config/KafkaConsumerConfig.java`
- Create: `src/main/java/com/template/batchconsumer/config/ApplicationConfig.java`
- Test: `src/test/java/com/template/batchconsumer/config/KafkaConsumerConfigTest.java`

**Interfaces:**
- Consumes: everything from Tasks 4–9 (`ConsumerOrchestrationService`, `SampleMessageProcessor`, `SamplePayload`, `KafkaListenerLifecycleAdapter`, `SampleKafkaListenerAdapter`'s expected bean name `sampleKafkaListenerContainerFactory`, `ConsumerLifecycleEndpoint`), plus `NonRetryableProcessingException` (Task 2), plus properties `consumer.sample.id`/`topic`/`concurrency`/`retry.*` (Task 1's `application.yml`).
- Produces: a fully wired Spring context — the beans `sampleConsumerFactory`, `sampleKafkaListenerContainerFactory`, `kafkaErrorHandler`, `consumerLifecycleControlPort`, `sampleConsumerOrchestrationService`, `sampleConsumeMessageUseCase`, `consumerLifecycleUseCase`, `consumerLifecycleEndpoint`. Task 12's integration tests and Task 13's OTel test run against this fully-assembled context.

Two files, split by responsibility: `KafkaConsumerConfig` owns the Kafka-specific plumbing (consumer factory, container factory, error handler with retry/DLT metrics); `ApplicationConfig` owns wiring the hexagonal application beans together. The spec's package sketch also mentions an `ObservabilityConfig` — it isn't created as a separate file because there's nothing to put in it: `micrometer-registry-prometheus` being on the classpath is enough for Spring Boot to auto-configure the `MeterRegistry` bean Task 5 and this task inject; an empty class just to match the sketch would violate the no-placeholder rule.

- [ ] **Step 1: Write the failing test for the error handler bean**

This test only checks the bean is constructed and wired correctly (retry listener registered). It deliberately does **not** try to simulate Spring Kafka's internal retry/backoff state machine in isolation — that's fragile to test via mocks and is already verified end-to-end, against a real broker, by Task 12's integration tests (`nonRetryableFailureIsPublishedToDltTopicImmediately`, `retryableFailureExhaustsRetriesAndLandsInDlt`).

```java
package com.template.batchconsumer.config;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class KafkaConsumerConfigTest {

    private final KafkaConsumerConfig config = new KafkaConsumerConfig();

    @Test
    @SuppressWarnings("unchecked")
    void errorHandlerRegistersARetryListenerForMetrics() {
        KafkaTemplate<Object, Object> kafkaTemplate = mock(KafkaTemplate.class);
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

        DefaultErrorHandler errorHandler = config.kafkaErrorHandler(
                kafkaTemplate, meterRegistry, "sample-consumer", 3, 1000L, 2.0, 10000L);

        assertThat(errorHandler.retryListeners()).isNotEmpty();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=KafkaConsumerConfigTest`
Expected: FAIL — `cannot find symbol: class KafkaConsumerConfig`

- [ ] **Step 3: Implement `KafkaConsumerConfig`**

```java
package com.template.batchconsumer.config;

import com.template.batchconsumer.application.sample.SamplePayload;
import com.template.batchconsumer.domain.exception.NonRetryableProcessingException;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.ExponentialBackOffWithMaxRetries;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, SamplePayload> sampleConsumerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${consumer.sample.id}") String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        JsonDeserializer<SamplePayload> valueDeserializer = new JsonDeserializer<>(SamplePayload.class);
        valueDeserializer.addTrustedPackages(SamplePayload.class.getPackageName());

        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), valueDeserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, SamplePayload> sampleKafkaListenerContainerFactory(
            ConsumerFactory<String, SamplePayload> sampleConsumerFactory,
            DefaultErrorHandler kafkaErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, SamplePayload> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(sampleConsumerFactory);
        factory.setCommonErrorHandler(kafkaErrorHandler);
        return factory;
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(
            KafkaTemplate<Object, Object> kafkaTemplate,
            MeterRegistry meterRegistry,
            @Value("${consumer.sample.id}") String consumerId,
            @Value("${consumer.sample.retry.max-attempts}") int maxAttempts,
            @Value("${consumer.sample.retry.initial-interval-ms}") long initialIntervalMs,
            @Value("${consumer.sample.retry.multiplier}") double multiplier,
            @Value("${consumer.sample.retry.max-interval-ms}") long maxIntervalMs) {

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(maxAttempts);
        backOff.setInitialInterval(initialIntervalMs);
        backOff.setMultiplier(multiplier);
        backOff.setMaxInterval(maxIntervalMs);

        DeadLetterPublishingRecoverer dltRecoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
        ConsumerRecordRecoverer countingRecoverer = (record, exception) -> {
            meterRegistry.counter("consumer.messages.dlt", "consumer", consumerId).increment();
            dltRecoverer.accept(record, exception);
        };

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(countingRecoverer, backOff);
        errorHandler.addNotRetryableExceptions(NonRetryableProcessingException.class);
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) ->
                meterRegistry.counter("consumer.messages.retried", "consumer", consumerId).increment());
        return errorHandler;
    }
}
```

*(If `errorHandler.retryListeners()` isn't the exact accessor name on the resolved Spring Kafka version, adjust the test in Step 1 to assert on observable behavior instead — e.g. invoke a retry and check the meter registry counter increments.)*

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q test -Dtest=KafkaConsumerConfigTest`
Expected: PASS

- [ ] **Step 5: Write `ApplicationConfig`**

```java
package com.template.batchconsumer.config;

import com.template.batchconsumer.adapter.out.kafka.KafkaListenerLifecycleAdapter;
import com.template.batchconsumer.application.port.in.ConsumeMessageUseCase;
import com.template.batchconsumer.application.port.in.ConsumerLifecycleUseCase;
import com.template.batchconsumer.application.port.out.ConsumerLifecycleControlPort;
import com.template.batchconsumer.application.sample.SampleMessageProcessor;
import com.template.batchconsumer.application.sample.SamplePayload;
import com.template.batchconsumer.application.service.ConsumerOrchestrationService;
import com.template.batchconsumer.adapter.in.web.ConsumerLifecycleEndpoint;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

@Configuration
public class ApplicationConfig {

    @Bean
    public ConsumerLifecycleControlPort consumerLifecycleControlPort(KafkaListenerEndpointRegistry registry) {
        return new KafkaListenerLifecycleAdapter(registry);
    }

    @Bean
    public ConsumerOrchestrationService<SamplePayload> sampleConsumerOrchestrationService(
            @Value("${consumer.sample.id}") String consumerId,
            MeterRegistry meterRegistry,
            ConsumerLifecycleControlPort consumerLifecycleControlPort) {
        return new ConsumerOrchestrationService<>(
                consumerId, new SampleMessageProcessor(), meterRegistry, consumerLifecycleControlPort);
    }

    @Bean
    public ConsumeMessageUseCase<SamplePayload> sampleConsumeMessageUseCase(
            ConsumerOrchestrationService<SamplePayload> sampleConsumerOrchestrationService) {
        return sampleConsumerOrchestrationService;
    }

    @Bean
    public ConsumerLifecycleUseCase consumerLifecycleUseCase(
            ConsumerOrchestrationService<SamplePayload> sampleConsumerOrchestrationService) {
        return sampleConsumerOrchestrationService;
    }

    @Bean
    public ConsumerLifecycleEndpoint consumerLifecycleEndpoint(ConsumerLifecycleUseCase consumerLifecycleUseCase) {
        return new ConsumerLifecycleEndpoint(consumerLifecycleUseCase);
    }
}
```

- [ ] **Step 6: Run the full context smoke test**

Run: `mvn -q test -Dtest=ApplicationContextTest`
Expected: PASS — the full bean graph (Tasks 4–10) now wires up successfully with no circular or missing dependencies

- [ ] **Step 7: Run the architecture test**

Run: `mvn -q test -Dtest=HexagonalArchitectureTest`
Expected: PASS (`config` depends on every other layer, which rule 5 allows; nothing depends on `config`)

- [ ] **Step 8: Commit**

```bash
git add src/main/java/com/template/batchconsumer/config src/test/java/com/template/batchconsumer/config
git commit -m "feat: wire Kafka consumer factory, error handler, and application beans"
```

---

## Task 11: BlockHound guard

**Files:**
- Test: `src/test/java/com/template/batchconsumer/architecture/BlockHoundGuardTest.java`

**Interfaces:**
- Consumes: `blockhound-junit-platform` (Task 1's pom.xml) — this dependency self-registers a JUnit Platform `TestExecutionListener` that installs BlockHound for the whole test run; no configuration code is needed beyond the dependency.
- Produces: proof that BlockHound is actually active, guarding the single-blocking-point invariant (spec Decision Log #3) for every test task that follows.

- [ ] **Step 1: Write the guard test**

```java
package com.template.batchconsumer.architecture;

import org.junit.jupiter.api.Test;
import reactor.blockhound.BlockingOperationError;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BlockHoundGuardTest {

    @Test
    void blockHoundDetectsBlockingCallOnNonBlockingScheduler() {
        Mono<String> blockingMono = Mono.fromCallable(() -> {
                    Thread.sleep(10);
                    return "done";
                })
                .subscribeOn(Schedulers.parallel());

        assertThatThrownBy(blockingMono::block)
                .isInstanceOf(BlockingOperationError.class);
    }
}
```

- [ ] **Step 2: Run the test**

Run: `mvn -q test -Dtest=BlockHoundGuardTest`
Expected: PASS. If it instead fails with the `Mono` completing normally (no exception thrown), BlockHound isn't installed — check the `blockhound-junit-platform` dependency and the surefire `argLine` from Task 1 are both present.

- [ ] **Step 3: Commit**

```bash
git add src/test/java/com/template/batchconsumer/architecture/BlockHoundGuardTest.java
git commit -m "test: prove BlockHound is active and guarding the reactive chain"
```

---

## Task 12: Docker Compose and Testcontainers integration tests

**Files:**
- Create: `docker-compose.yml`
- Test: `src/test/java/com/template/batchconsumer/adapter/in/kafka/SampleConsumerIntegrationTest.java`

**Interfaces:**
- Consumes: the fully wired Spring context (Task 10), `SamplePayload`/`SampleMessageProcessor`'s trigger strings (Task 6), the `-dlt` topic-suffix convention (spec Decision Log #6).
- Produces: end-to-end proof the vertical slice works against a real (containerized) Kafka broker — normal processing, non-retryable failure straight to DLT, retryable failure exhausting retries into DLT.

- [ ] **Step 1: Write `docker-compose.yml`**

```yaml
services:
  kafka:
    image: apache/kafka:3.9.0
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

- [ ] **Step 2: Write the failing integration test for the non-retryable path**

```java
package com.template.batchconsumer.adapter.in.kafka;

import com.template.batchconsumer.application.sample.SamplePayload;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest
class SampleConsumerIntegrationTest {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.0");

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Test
    void nonRetryableFailureIsPublishedToDltTopicImmediately() {
        try (var dltConsumer = KafkaTestUtils.getConsumer(Map.of(
                "bootstrap.servers", KAFKA.getBootstrapServers(),
                "group.id", "dlt-test-non-retryable",
                "auto.offset.reset", "earliest",
                "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer"))) {
            dltConsumer.subscribe(List.of("sample-events-dlt"));

            kafkaTemplate.send(new ProducerRecord<>(
                    "sample-events", "key-fatal", new SamplePayload("id-fatal", "FAIL_FATAL")));

            var records = dltConsumer.poll(Duration.ofSeconds(15));
            assertThat(records.count()).isGreaterThanOrEqualTo(1);
        }
    }
}
```

*(If `org.testcontainers.kafka.KafkaContainer` doesn't resolve against Testcontainers 2.0.5, check the current Testcontainers Kafka module docs — the container class may still be `org.testcontainers.containers.KafkaContainer` depending on how the 2.x module split landed.)*

- [ ] **Step 3: Run test to verify it fails**

Run: `mvn -q test -Dtest=SampleConsumerIntegrationTest`
Expected: FAIL initially if Docker/Testcontainers isn't reachable in this environment — confirm Docker is running, then re-run. Once Docker is available, this should PASS immediately since Tasks 6–10 already implement the described behavior; if it fails for a different reason (e.g. no message arrives on `sample-events-dlt`), that's a real bug to fix before proceeding, not an expected red state to code past.

- [ ] **Step 4: Add the retryable-exhaustion and happy-path tests**

Append to `SampleConsumerIntegrationTest`:

```java
    @Test
    void retryableFailureExhaustsRetriesAndLandsInDlt() {
        try (var dltConsumer = KafkaTestUtils.getConsumer(Map.of(
                "bootstrap.servers", KAFKA.getBootstrapServers(),
                "group.id", "dlt-test-retryable",
                "auto.offset.reset", "earliest",
                "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer"))) {
            dltConsumer.subscribe(List.of("sample-events-dlt"));

            kafkaTemplate.send(new ProducerRecord<>(
                    "sample-events", "key-retry", new SamplePayload("id-retry", "FAIL_RETRYABLE")));

            // 3 attempts with 1s/2s backoff (application.yml) should exhaust well within 20s.
            var records = dltConsumer.poll(Duration.ofSeconds(20));
            assertThat(records.count()).isGreaterThanOrEqualTo(1);
        }
    }

    @Test
    void successfulMessageDoesNotReachDltTopic() {
        try (var dltConsumer = KafkaTestUtils.getConsumer(Map.of(
                "bootstrap.servers", KAFKA.getBootstrapServers(),
                "group.id", "dlt-test-success",
                "auto.offset.reset", "earliest",
                "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer"))) {
            dltConsumer.subscribe(List.of("sample-events-dlt"));

            kafkaTemplate.send(new ProducerRecord<>(
                    "sample-events", "key-ok", new SamplePayload("id-ok", "hello")));

            var records = dltConsumer.poll(Duration.ofSeconds(5));
            assertThat(records.count()).isZero();
        }
    }
```

- [ ] **Step 5: Run the full test class**

Run: `mvn -q test -Dtest=SampleConsumerIntegrationTest`
Expected: PASS, 3 tests

- [ ] **Step 6: Commit**

```bash
git add docker-compose.yml src/test/java/com/template/batchconsumer/adapter/in/kafka/SampleConsumerIntegrationTest.java
git commit -m "test: add Testcontainers Kafka integration tests for retry/DLT paths"
```

---

## Task 13: OpenTelemetry context-propagation verification

**Files:**
- Test: `src/test/java/com/template/batchconsumer/observability/OtelContextPropagationTest.java`

**Interfaces:**
- Consumes: `MessageProcessor<T>` (Task 4), `MessageEnvelope<T>`/`ProcessingOutcome` (Task 2), `opentelemetry-sdk-testing`'s `InMemorySpanExporter`, `opentelemetry-reactor-3.1`'s `ContextPropagationOperator` (Task 1's pom.xml).
- Produces: standing proof (spec Decision Log #8) that a span created inside the reactive body, after a thread hop, nests correctly under the span active at the `.block()` call site — the same mechanism the production OTel javaagent relies on.

This test builds its own `OpenTelemetrySdk` + `InMemorySpanExporter` (not the shaded javaagent, which can't be attached inside a Surefire-run JUnit test easily) and uses the same reactor context-propagation library the javaagent uses internally, so a pass here gives strong confidence the production agent-attached scenario also works.

- [ ] **Step 1: Write the failing test**

```java
package com.template.batchconsumer.observability;

import com.template.batchconsumer.application.port.in.MessageProcessor;
import com.template.batchconsumer.domain.model.MessageEnvelope;
import com.template.batchconsumer.domain.model.ProcessingOutcome;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.instrumentation.reactor.v3_1.ContextPropagationOperator;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class OtelContextPropagationTest {

    private static InMemorySpanExporter spanExporter;
    private static Tracer tracer;

    @BeforeAll
    static void setUpOpenTelemetry() {
        spanExporter = InMemorySpanExporter.create();
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                .build();
        OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .build();
        tracer = openTelemetry.getTracer("test");
        ContextPropagationOperator.INSTANCE.registerOnEachOperator();
    }

    @AfterAll
    static void tearDownOpenTelemetry() {
        ContextPropagationOperator.INSTANCE.resetOnEachOperator();
    }

    /**
     * Stand-in for a real MessageProcessor that makes an instrumented downstream call —
     * creates a child span from inside the reactive body, on whatever thread Reactor
     * schedules that stage on (here, deliberately a different thread via boundedElastic).
     */
    private static final class SpanCreatingProcessor implements MessageProcessor<String> {
        @Override
        public Mono<ProcessingOutcome> process(MessageEnvelope<String> envelope) {
            return Mono.fromCallable(() -> {
                        Span childSpan = tracer.spanBuilder("downstream-call").startSpan();
                        childSpan.end();
                        return ProcessingOutcome.SUCCESS;
                    })
                    .subscribeOn(Schedulers.boundedElastic());
        }
    }

    @Test
    void spanCreatedInsideReactiveBodyNestsUnderKafkaConsumerSpan() {
        spanExporter.reset();
        MessageProcessor<String> processor = new SpanCreatingProcessor();
        MessageEnvelope<String> envelope =
                new MessageEnvelope<>("sample-consumer", "sample-events", 0, 0L, "key", "payload");

        Span consumerSpan = tracer.spanBuilder("sample-events process")
                .setSpanKind(SpanKind.CONSUMER)
                .startSpan();
        try (Scope scope = consumerSpan.makeCurrent()) {
            // Mirrors the production bridge: the Mono is built and subscribed while the
            // Kafka consumer span is current, then blocked on — same as
            // SampleKafkaListenerAdapter.onMessage's single blocking bridge point.
            processor.process(envelope).block(Duration.ofSeconds(5));
        } finally {
            consumerSpan.end();
        }

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        SpanData consumerSpanData = spans.stream()
                .filter(s -> s.getName().equals("sample-events process"))
                .findFirst()
                .orElseThrow();
        SpanData childSpanData = spans.stream()
                .filter(s -> s.getName().equals("downstream-call"))
                .findFirst()
                .orElseThrow();

        assertThat(childSpanData.getParentSpanId()).isEqualTo(consumerSpanData.getSpanId());
    }
}
```

*(If `io.opentelemetry.instrumentation.reactor.v3_1.ContextPropagationOperator` doesn't resolve, check the `opentelemetry-reactor-3.1` artifact's current package name against `opentelemetry-instrumentation-bom-alpha:2.28.1-alpha`'s docs — alpha packages occasionally shift their version-suffixed package path across releases.)*

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=OtelContextPropagationTest`
Expected: FAIL initially (no `ContextPropagationOperator` registered yet is not the failure mode here — the class/method not existing would be a compile failure; once it compiles, expect this to already PASS if the reactor instrumentation library correctly bridges context, since the mechanism under test is the library's, not code we're writing). If it fails with the child span's parent not matching, that's the real finding this task exists to catch — do not "fix" it by loosening the assertion; investigate the reactor instrumentation registration instead.

- [ ] **Step 3: Run test to verify it passes**

Run: `mvn -q test -Dtest=OtelContextPropagationTest`
Expected: PASS

- [ ] **Step 4: Run the full test suite one final time**

Run: `mvn -q test`
Expected: `BUILD SUCCESS`, all tests across all 13 tasks green

- [ ] **Step 5: Commit**

```bash
git add src/test/java/com/template/batchconsumer/observability/OtelContextPropagationTest.java
git commit -m "test: verify OTel context propagates across the block() bridge"
```
