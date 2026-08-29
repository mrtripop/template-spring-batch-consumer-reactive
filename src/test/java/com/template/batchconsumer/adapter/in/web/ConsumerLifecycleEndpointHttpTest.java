package com.template.batchconsumer.adapter.in.web;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.reactive.server.WebTestClient;

import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

/**
 * Exercises {@link ConsumerLifecycleEndpoint} over real HTTP via Spring Boot Actuator's web
 * transport (an actual running server on a random port), unlike {@link
 * ConsumerLifecycleEndpointTest}, whose 6 cases all call the endpoint's Java methods directly and
 * so never exercise HTTP status codes, path/selector binding, or JSON serialization. In
 * particular this is what actually proves the Fix 3 {@code NoSuchElementException} -> HTTP 404
 * translation works for a real client, not just inside a mocked unit test.
 *
 * <p>{@code @DirtiesContext(AFTER_CLASS)}: this context's real {@code @KafkaListener} container
 * has no reachable broker configured (no Testcontainers broker is started for this test, unlike
 * {@code SampleConsumerIntegrationTest}), so its consumer threads retry connecting indefinitely.
 * Without this annotation, Spring's test context cache would keep that context — and its
 * perpetually-retrying background threads — alive for the rest of the surefire JVM/fork, adding
 * CPU/log-volume contention across every other test class that runs afterwards. Marking the
 * context dirty after this class ensures Spring closes it (stopping the listener container and
 * its threads) as soon as this class's tests finish.
 */
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = RANDOM_PORT)
class ConsumerLifecycleEndpointHttpTest {

    private static final String SAMPLE_CONSUMER_ID = "sample-consumer";
    private static final String UNKNOWN_CONSUMER_ID = "does-not-exist";

    @LocalServerPort
    private int port;

    private WebTestClient client;

    @BeforeEach
    void setUp() {
        client = WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .build();
    }

    @Test
    void getConsumersListReturnsOkWithSampleConsumer() {
        client.get().uri("/actuator/consumers")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$[0].consumerId").isEqualTo(SAMPLE_CONSUMER_ID)
                .jsonPath("$[0].state").exists();
    }

    @Test
    void getSingleConsumerStatusReturnsOkWithConsumerId() {
        client.get().uri("/actuator/consumers/{id}", SAMPLE_CONSUMER_ID)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.consumerId").isEqualTo(SAMPLE_CONSUMER_ID)
                .jsonPath("$.state").exists();
    }

    // RESUME is used here (rather than PAUSE/STOP) specifically because it's a safe no-op on an
    // already-running container — this test only needs to prove the write operation reaches the
    // listener container and returns 200 over real HTTP, without leaving the shared consumer
    // paused/stopped for whichever other test method in this class happens to run next.
    @Test
    void postActionOverHttpReturnsOk() {
        client.post().uri("/actuator/consumers/{id}/{action}", SAMPLE_CONSUMER_ID, "RESUME")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isOk();
    }

    @Test
    void getUnknownConsumerIdReturnsNotFound() {
        client.get().uri("/actuator/consumers/{id}", UNKNOWN_CONSUMER_ID)
                .exchange()
                .expectStatus().isNotFound();
    }

    @Test
    void postActionForUnknownConsumerIdReturnsNotFound() {
        client.post().uri("/actuator/consumers/{id}/{action}", UNKNOWN_CONSUMER_ID, "PAUSE")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isNotFound();
    }
}
