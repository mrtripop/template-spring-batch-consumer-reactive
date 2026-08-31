package com.template.batchconsumer.adapter.in.web;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.reactive.server.WebTestClient;

import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

/**
 * Exercises {@link ConsumerLifecycleEndpoint} over real HTTP, unlike {@link
 * ConsumerLifecycleEndpointTest}'s Java-method-level mocks.
 */
// This context's @KafkaListener has no reachable broker, so its threads retry forever unless
// the context is torn down after this class.
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = RANDOM_PORT)
@DisplayName("Consumer lifecycle endpoint (real HTTP)")
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
    @DisplayName("GET the consumer list returns 200 with the sample consumer")
    void getConsumersListReturnsOkWithSampleConsumer() {
        // Act & Assert
        client.get().uri("/actuator/consumers")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$[0].consumerId").isEqualTo(SAMPLE_CONSUMER_ID)
                .jsonPath("$[0].state").exists();
    }

    @Test
    @DisplayName("GET a single known consumer returns 200 with its status")
    void getSingleConsumerStatusReturnsOkWithConsumerId() {
        // Act & Assert
        client.get().uri("/actuator/consumers/{id}", SAMPLE_CONSUMER_ID)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.consumerId").isEqualTo(SAMPLE_CONSUMER_ID)
                .jsonPath("$.state").exists();
    }

    // RESUME is a safe no-op on an already-running container, so this doesn't disturb other tests.
    @Test
    @DisplayName("POST an action for a known consumer returns 200")
    void postActionOverHttpReturnsOk() {
        // Act & Assert
        client.post().uri("/actuator/consumers/{id}/{action}", SAMPLE_CONSUMER_ID, "RESUME")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isOk();
    }

    @Test
    @DisplayName("GET an unknown consumer returns 404")
    void getUnknownConsumerIdReturnsNotFound() {
        // Act & Assert
        client.get().uri("/actuator/consumers/{id}", UNKNOWN_CONSUMER_ID)
                .exchange()
                .expectStatus().isNotFound();
    }

    @Test
    @DisplayName("POST an action for an unknown consumer returns 404")
    void postActionForUnknownConsumerIdReturnsNotFound() {
        // Act & Assert
        client.post().uri("/actuator/consumers/{id}/{action}", UNKNOWN_CONSUMER_ID, "PAUSE")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isNotFound();
    }
}
