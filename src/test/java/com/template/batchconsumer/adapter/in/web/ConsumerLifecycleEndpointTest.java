package com.template.batchconsumer.adapter.in.web;

import com.template.batchconsumer.application.port.in.ConsumerLifecycleUseCase;
import com.template.batchconsumer.domain.model.ConsumerStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.actuate.endpoint.web.WebEndpointResponse;

import java.time.Instant;
import java.util.List;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@DisplayName("Consumer lifecycle endpoint (Java-method level)")
class ConsumerLifecycleEndpointTest {

    private static final String CONSUMER_ID = "sample-consumer";
    private static final String UNKNOWN_CONSUMER_ID = "does-not-exist";

    private final ConsumerLifecycleUseCase useCase = Mockito.mock(ConsumerLifecycleUseCase.class);
    private final ConsumerLifecycleEndpoint endpoint = new ConsumerLifecycleEndpoint(useCase);

    @Nested
    @DisplayName("read operations")
    class ReadOperations {

        @Test
        @DisplayName("statuses() delegates to the use case")
        void statusesDelegatesToUseCase() {
            // Arrange
            ConsumerStatus status = new ConsumerStatus(CONSUMER_ID, ConsumerStatus.State.RUNNING, Instant.now());
            when(useCase.statuses()).thenReturn(List.of(status));

            // Act & Assert
            assertThat(endpoint.statuses()).containsExactly(status);
        }

        @Test
        @DisplayName("status(id) delegates to the use case for a known consumer")
        void statusDelegatesToUseCaseForGivenId() {
            // Arrange
            ConsumerStatus status = new ConsumerStatus(CONSUMER_ID, ConsumerStatus.State.PAUSED, Instant.now());
            when(useCase.status(CONSUMER_ID)).thenReturn(status);

            // Act
            WebEndpointResponse<ConsumerStatus> response = endpoint.status(CONSUMER_ID);

            // Assert
            assertThat(response.getStatus()).isEqualTo(WebEndpointResponse.STATUS_OK);
            assertThat(response.getBody()).isEqualTo(status);
        }

        @Test
        @DisplayName("status(id) returns 404 for an unknown consumer")
        void statusReturnsNotFoundForUnknownConsumerId() {
            // Arrange
            when(useCase.status(UNKNOWN_CONSUMER_ID)).thenThrow(new NoSuchElementException("no such container"));

            // Act
            WebEndpointResponse<ConsumerStatus> response = endpoint.status(UNKNOWN_CONSUMER_ID);

            // Assert
            assertThat(response.getStatus()).isEqualTo(WebEndpointResponse.STATUS_NOT_FOUND);
        }
    }

    @Nested
    @DisplayName("write operations")
    class WriteOperations {

        @Test
        @DisplayName("PAUSE calls pause() and returns 200")
        void applyActionPauseCallsPause() {
            // Act
            WebEndpointResponse<Void> response = endpoint.applyAction(CONSUMER_ID, ConsumerLifecycleEndpoint.Action.PAUSE);

            // Assert
            verify(useCase).pause(CONSUMER_ID);
            assertThat(response.getStatus()).isEqualTo(WebEndpointResponse.STATUS_OK);
        }

        @Test
        @DisplayName("RESUME calls resume()")
        void applyActionResumeCallsResume() {
            // Act
            endpoint.applyAction(CONSUMER_ID, ConsumerLifecycleEndpoint.Action.RESUME);

            // Assert
            verify(useCase).resume(CONSUMER_ID);
        }

        @Test
        @DisplayName("STOP calls stop()")
        void applyActionStopCallsStop() {
            // Act
            endpoint.applyAction(CONSUMER_ID, ConsumerLifecycleEndpoint.Action.STOP);

            // Assert
            verify(useCase).stop(CONSUMER_ID);
        }

        @Test
        @DisplayName("START calls start()")
        void applyActionStartCallsStart() {
            // Act
            endpoint.applyAction(CONSUMER_ID, ConsumerLifecycleEndpoint.Action.START);

            // Assert
            verify(useCase).start(CONSUMER_ID);
        }

        @Test
        @DisplayName("an action for an unknown consumer returns 404")
        void applyActionReturnsNotFoundForUnknownConsumerId() {
            // Arrange
            doThrow(new NoSuchElementException("no such container")).when(useCase).pause(UNKNOWN_CONSUMER_ID);

            // Act
            WebEndpointResponse<Void> response =
                    endpoint.applyAction(UNKNOWN_CONSUMER_ID, ConsumerLifecycleEndpoint.Action.PAUSE);

            // Assert
            assertThat(response.getStatus()).isEqualTo(WebEndpointResponse.STATUS_NOT_FOUND);
        }
    }
}
