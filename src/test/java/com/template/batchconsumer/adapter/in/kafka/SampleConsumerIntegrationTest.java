package com.template.batchconsumer.adapter.in.kafka;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.template.batchconsumer.application.sample.SampleMessageProcessor;
import com.template.batchconsumer.application.sample.SamplePayload;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
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
@DisplayName("Sample consumer end-to-end (real Kafka broker)")
class SampleConsumerIntegrationTest {

    private static final String TOPIC = "sample-events";
    private static final String DLT_TOPIC = "sample-events-dlt";

    // Only set if unset, so this doesn't override a template adopter's own Docker Engine API
    // version — pass -Dapi.version=<yours> (or DOCKER_API_VERSION) to use a different value.
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

    private KafkaConsumer<String, byte[]> newDltConsumer(String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return new KafkaConsumer<>(props);
    }

    // Seeks to the current tail so a fresh "earliest" consumer group only sees records this
    // test's own message produces, not leftovers from another @Test method (JUnit doesn't
    // guarantee method order, and @Container is one broker shared by every test here).
    private void seekToTail(KafkaConsumer<String, byte[]> consumer) {
        long assignmentDeadline = System.currentTimeMillis() + Duration.ofSeconds(10).toMillis();
        while (consumer.assignment().isEmpty() && System.currentTimeMillis() < assignmentDeadline) {
            consumer.poll(Duration.ofMillis(100));
        }
        assertThat(consumer.assignment()).as("DLT consumer partition assignment").isNotEmpty();
        consumer.seekToEnd(consumer.assignment());
    }

    // Polls until a record with the given key shows up, or the timeout elapses. Checking the key
    // (not just "some record arrived") ensures each test only counts its own message.
    private boolean pollForKey(KafkaConsumer<String, byte[]> consumer, String expectedKey, Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, byte[]> record : records.records(DLT_TOPIC)) {
                if (expectedKey.equals(record.key())) {
                    return true;
                }
            }
        }
        return false;
    }

    @Nested
    @DisplayName("a non-retryable failure")
    class NonRetryableFailure {

        @Test
        @DisplayName("is published to the DLT topic")
        void nonRetryableFailureIsPublishedToDltTopicImmediately() {
            // Arrange
            String key = "key-fatal";
            try (var dltConsumer = newDltConsumer("dlt-test-non-retryable")) {
                dltConsumer.subscribe(List.of(DLT_TOPIC));
                seekToTail(dltConsumer);

                // Act
                kafkaTemplate.send(new ProducerRecord<>(
                        TOPIC, key, new SamplePayload("id-fatal", SampleMessageProcessor.NON_RETRYABLE_TRIGGER)));

                // Assert — 45s: the bulk of this test's elapsed time is Testcontainers/consumer-group
                // startup overhead common to every test in this class (observed up to ~28s under
                // machine load), not the DLT round-trip itself.
                assertThat(pollForKey(dltConsumer, key, Duration.ofSeconds(45)))
                        .as("DLT record with key '%s' produced by this test's own message", key)
                        .isTrue();
            }
        }
    }

    @Nested
    @DisplayName("a retryable failure")
    class RetryableFailure {

        @Test
        @DisplayName("exhausts retries and lands in the DLT")
        void retryableFailureExhaustsRetriesAndLandsInDlt() {
            // Arrange
            String key = "key-retry";
            try (var dltConsumer = newDltConsumer("dlt-test-retryable")) {
                dltConsumer.subscribe(List.of(DLT_TOPIC));
                seekToTail(dltConsumer);

                // Act
                kafkaTemplate.send(new ProducerRecord<>(
                        TOPIC, key, new SamplePayload("id-retry", SampleMessageProcessor.RETRYABLE_TRIGGER)));

                // Assert — 4 total deliveries (1 initial + 3 retries) with 1s/2s/4s backoff
                // (application.yml's consumer.sample.retry.max-attempts: 3) exhaust well within 20s.
                assertThat(pollForKey(dltConsumer, key, Duration.ofSeconds(20)))
                        .as("DLT record with key '%s' produced by this test's own message", key)
                        .isTrue();
            }
        }
    }

    @Nested
    @DisplayName("a successful message")
    class SuccessfulMessage {

        @Test
        @DisplayName("never reaches the DLT topic")
        void successfulMessageDoesNotReachDltTopic() {
            // Arrange
            try (var dltConsumer = newDltConsumer("dlt-test-success")) {
                dltConsumer.subscribe(List.of(DLT_TOPIC));
                seekToTail(dltConsumer);

                // Act
                kafkaTemplate.send(new ProducerRecord<>(TOPIC, "key-ok", new SamplePayload("id-ok", "hello")));

                // Assert
                var records = dltConsumer.poll(Duration.ofSeconds(5));
                assertThat(records.count()).isZero();
            }
        }
    }

    // org.testcontainers.kafka.KafkaContainer's generated KAFKA_ADVERTISED_LISTENERS omits a
    // CONTROLLER entry, which crashes apache/kafka:3.9.0's combined-mode KRaft bootstrap
    // ("advertised.listeners cannot use the nonroutable meta-address 0.0.0.0"). This subclass
    // mirrors that class's wait-for-script approach but adds the missing CONTROLLER entry.
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
            // Advertise CONTROLLER alongside PLAINTEXT — see the class-level comment above.
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
