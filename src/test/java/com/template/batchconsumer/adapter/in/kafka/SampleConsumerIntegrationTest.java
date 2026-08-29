package com.template.batchconsumer.adapter.in.kafka;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.template.batchconsumer.application.sample.SamplePayload;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest
class SampleConsumerIntegrationTest {

    // This environment's Docker engine (Docker Desktop 4.78.0 / Engine 29.5.3) enforces a minimum
    // Docker Engine API version of 1.40 (confirmed via `docker version` and raw /vX.Y/info probes:
    // v1.39 and below get HTTP 400, v1.40+ succeeds). When "api.version" is unset, docker-java (the
    // client Testcontainers 1.20.1 bundles/shades) defaults to RemoteApiVersion.UNKNOWN_VERSION, an
    // auto-negotiation placeholder rather than a literal old version number — but this engine
    // rejects that unnegotiated request outright, so every DockerClientProviderStrategy fails with
    // "Could not find a valid Docker environment" even though `docker ps`/`docker info` work fine
    // directly. Pinning it here — before the @Container field below (or anything else in this class)
    // touches Testcontainers — fixes that without requiring any special flags on the documented
    // `./mvnw test` invocation. Only set it if not already configured, so this doesn't override a
    // template adopter's own Docker Engine API version (e.g. an older engine that needs a version
    // below 1.40): pass -Dapi.version=<your version> (or set the DOCKER_API_VERSION env var, if you
    // prefer) before running the tests to use a different value instead of this default.
    static {
        if (System.getProperty("api.version") == null) {
            System.setProperty("api.version", "1.41");
        }
    }

    @Container
    static final KafkaTestContainer KAFKA = new KafkaTestContainer("apache/kafka:3.9.0");

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;

    // Spring Kafka 4.1.1's KafkaTestUtils has no getConsumer(Map) overload (it was removed from
    // this version's test-utils surface, unlike what the plan's brief assumed) — so DLT-topic
    // probe consumers are built directly against the raw Kafka client instead.
    private KafkaConsumer<String, byte[]> newDltConsumer(String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return new KafkaConsumer<>(props);
    }

    @Test
    void nonRetryableFailureIsPublishedToDltTopicImmediately() {
        try (var dltConsumer = newDltConsumer("dlt-test-non-retryable")) {
            dltConsumer.subscribe(List.of("sample-events-dlt"));

            kafkaTemplate.send(new ProducerRecord<>(
                    "sample-events", "key-fatal", new SamplePayload("id-fatal", "FAIL_FATAL")));

            var records = dltConsumer.poll(Duration.ofSeconds(15));
            assertThat(records.count()).isGreaterThanOrEqualTo(1);
        }
    }

    @Test
    void retryableFailureExhaustsRetriesAndLandsInDlt() {
        try (var dltConsumer = newDltConsumer("dlt-test-retryable")) {
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
        try (var dltConsumer = newDltConsumer("dlt-test-success")) {
            dltConsumer.subscribe(List.of("sample-events-dlt"));

            // The @Container broker is a single static field shared by all three @Test methods in
            // this class, and JUnit 5 does not guarantee method execution order — so if the
            // retryable/non-retryable tests happen to run first, they leave real DLT records on
            // this topic. A brand-new "earliest" consumer group would otherwise read those leftover
            // records and produce a false positive here. Seeking to the current tail first scopes
            // the assertion to records produced by *this* test's message, matching what the test
            // actually intends to verify. Partition assignment only completes once the group
            // rebalance finishes, so poll in a loop until it does rather than a single fixed-delay
            // poll (which can race ahead of assignment and silently seek nothing).
            long assignmentDeadline = System.currentTimeMillis() + Duration.ofSeconds(10).toMillis();
            while (dltConsumer.assignment().isEmpty() && System.currentTimeMillis() < assignmentDeadline) {
                dltConsumer.poll(Duration.ofMillis(100));
            }
            assertThat(dltConsumer.assignment()).as("DLT consumer partition assignment").isNotEmpty();
            dltConsumer.seekToEnd(dltConsumer.assignment());

            kafkaTemplate.send(new ProducerRecord<>(
                    "sample-events", "key-ok", new SamplePayload("id-ok", "hello")));

            var records = dltConsumer.poll(Duration.ofSeconds(5));
            assertThat(records.count()).isZero();
        }
    }

    // The brief specifies `org.testcontainers.kafka.KafkaContainer` (Testcontainers 1.20.1's class
    // dedicated to the official apache/kafka native image — the OTHER Kafka class in this version,
    // org.testcontainers.containers.KafkaContainer, is hard-pinned to Confluent Platform image
    // versioning and outright rejects "apache/kafka:3.9.0" as an unsupported CP version string).
    //
    // That class's import and constructor resolve exactly as the brief wrote them, but starting the
    // container with it reproducibly fails in this environment: Kafka's own KRaft bootstrap crashes
    // with "advertised.listeners cannot use the nonroutable meta-address 0.0.0.0". Root-caused by
    // hand-running apache/kafka:3.9.0's entrypoint with the exact env vars that class generates: its
    // dynamically-computed KAFKA_ADVERTISED_LISTENERS includes PLAINTEXT and BROKER but omits a
    // CONTROLLER entry entirely. For a single-node combined broker+controller KRaft node, Kafka
    // 3.9.0 requires every listener named in controller.listener.names to also have an advertised
    // entry — if it's missing, it silently falls back to the raw (0.0.0.0) bind address from
    // `listeners` and then fails its own routability validation. Confirmed by manually re-running
    // the container with an explicit CONTROLLER entry added to KAFKA_ADVERTISED_LISTENERS: it boots
    // cleanly. This looks like a genuine gap in Testcontainers 1.20.1's apache/kafka wiring for
    // combined-mode KRaft, not anything introduced by this project's Tasks 6-10.
    //
    // This minimal GenericContainer subclass mirrors that library class's approach (wait for a
    // startup script to appear, then export the real KAFKA_ADVERTISED_LISTENERS once the mapped
    // host port is known, then hand off to the image's own /etc/kafka/docker/run) but adds the
    // missing CONTROLLER entry, which is the one-line fix that makes it work.
    private static final class KafkaTestContainer extends GenericContainer<KafkaTestContainer> {

        private static final int KAFKA_PORT = 9092;
        private static final int CONTROLLER_PORT = 9094;
        private static final String CLUSTER_ID = "4L6g3nShT-eMCtK--X86sw";
        private static final String START_SCRIPT_PATH = "/tmp/testcontainers_start.sh";

        KafkaTestContainer(String dockerImageName) {
            super(DockerImageName.parse(dockerImageName));
            withExposedPorts(KAFKA_PORT);
            withEnv("CLUSTER_ID", CLUSTER_ID);
            withEnv("KAFKA_NODE_ID", "1");
            withEnv("KAFKA_PROCESS_ROLES", "broker,controller");
            withEnv("KAFKA_LISTENERS",
                    "PLAINTEXT://0.0.0.0:" + KAFKA_PORT + ",CONTROLLER://0.0.0.0:" + CONTROLLER_PORT);
            withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT");
            withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER");
            withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT");
            withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:" + CONTROLLER_PORT);
            withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");
            withCommand("sh", "-c",
                    "while [ ! -f " + START_SCRIPT_PATH + " ]; do sleep 0.1; done; " + START_SCRIPT_PATH);
            waitingFor(Wait.forLogMessage(".*Transitioning from RECOVERY to RUNNING.*", 1));
        }

        @Override
        protected void containerIsStarting(InspectContainerResponse containerInfo) {
            // Advertise CONTROLLER alongside PLAINTEXT (unlike Testcontainers' own
            // org.testcontainers.kafka.KafkaContainer, which only advertises PLAINTEXT/BROKER —
            // see the class-level comment above for why that omission crashes this image/version).
            String advertisedListeners = "PLAINTEXT://" + getHost() + ":" + getMappedPort(KAFKA_PORT)
                    + ",CONTROLLER://localhost:" + CONTROLLER_PORT;
            String script = "#!/bin/bash\n"
                    + "export KAFKA_ADVERTISED_LISTENERS=" + advertisedListeners + "\n"
                    + "/etc/kafka/docker/run\n";
            copyFileToContainer(Transferable.of(script, 0777), START_SCRIPT_PATH);
        }

        String getBootstrapServers() {
            return getHost() + ":" + getMappedPort(KAFKA_PORT);
        }
    }
}
