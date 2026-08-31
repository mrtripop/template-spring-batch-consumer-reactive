package com.template.batchconsumer.application.service;

import com.template.batchconsumer.application.metrics.MetricNames;
import com.template.batchconsumer.application.port.in.MessageProcessor;
import com.template.batchconsumer.application.port.out.ConsumerLifecycleControlPort;
import com.template.batchconsumer.domain.exception.NonRetryableProcessingException;
import com.template.batchconsumer.domain.exception.RetryableProcessingException;
import com.template.batchconsumer.domain.model.ConsumerStatus;
import com.template.batchconsumer.domain.model.MessageEnvelope;
import com.template.batchconsumer.domain.model.ProcessingOutcome;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@DisplayName("Consumer orchestration service")
class ConsumerOrchestrationServiceTest {

    private static final String CONSUMER_ID = "sample-consumer";
    private static final String TOPIC = "sample-events";

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
        service = new ConsumerOrchestrationService<>(CONSUMER_ID, messageProcessor, meterRegistry, lifecycleControlPort);
    }

    /** Builds an envelope for this consumer/topic, varying only offset and key per test. */
    private MessageEnvelope<String> envelope(long offset, String key) {
        return new MessageEnvelope<>(CONSUMER_ID, TOPIC, 0, offset, key, "payload");
    }

    @Nested
    @DisplayName("consume() outcomes")
    class ConsumeOutcomes {

        @Test
        @DisplayName("a successful process() records SUCCESS and the processing duration")
        void consumeRecordsSuccessOutcomeAndDuration() {
            // Arrange
            MessageEnvelope<String> envelope = envelope(1L, "k1");
            when(messageProcessor.process(envelope)).thenReturn(Mono.just(ProcessingOutcome.SUCCESS));

            // Act
            StepVerifier.create(service.consume(envelope))
                    .expectNext(ProcessingOutcome.SUCCESS)
                    .verifyComplete();

            // Assert
            assertThat(meterRegistry.counter(MetricNames.MESSAGES_PROCESSED, MetricNames.TAG_CONSUMER, CONSUMER_ID, MetricNames.TAG_OUTCOME, "SUCCESS").count())
                    .isEqualTo(1.0);
            assertThat(meterRegistry.timer(MetricNames.PROCESSING_DURATION, MetricNames.TAG_CONSUMER, CONSUMER_ID).count())
                    .isEqualTo(1);
        }

        @Test
        @DisplayName("a RetryableProcessingException records RETRYABLE_FAILURE and propagates the error")
        void consumeRecordsRetryableFailureOutcomeAndPropagatesError() {
            // Arrange
            MessageEnvelope<String> envelope = envelope(2L, "k2");
            RetryableProcessingException failure = new RetryableProcessingException("transient");
            when(messageProcessor.process(envelope)).thenReturn(Mono.error(failure));

            // Act
            StepVerifier.create(service.consume(envelope))
                    .expectErrorMatches(error -> error == failure)
                    .verify();

            // Assert
            assertThat(meterRegistry.counter(MetricNames.MESSAGES_PROCESSED, MetricNames.TAG_CONSUMER, CONSUMER_ID, MetricNames.TAG_OUTCOME, "RETRYABLE_FAILURE").count())
                    .isEqualTo(1.0);
        }

        @Test
        @DisplayName("a NonRetryableProcessingException records NON_RETRYABLE_FAILURE and propagates the error")
        void consumeRecordsNonRetryableFailureOutcomeAndPropagatesError() {
            // Arrange
            MessageEnvelope<String> envelope = envelope(3L, "k3");
            NonRetryableProcessingException failure = new NonRetryableProcessingException("fatal");
            when(messageProcessor.process(envelope)).thenReturn(Mono.error(failure));

            // Act
            StepVerifier.create(service.consume(envelope))
                    .expectErrorMatches(error -> error == failure)
                    .verify();

            // Assert
            assertThat(meterRegistry.counter(MetricNames.MESSAGES_PROCESSED, MetricNames.TAG_CONSUMER, CONSUMER_ID, MetricNames.TAG_OUTCOME, "NON_RETRYABLE_FAILURE").count())
                    .isEqualTo(1.0);
        }
    }

    @Nested
    @DisplayName("lifecycle delegation")
    class LifecycleDelegation {

        @Test
        @DisplayName("start/pause/resume/stop/status/statuses all delegate to the control port")
        void lifecycleMethodsDelegateToControlPort() {
            // Arrange
            ConsumerStatus expectedStatus = new ConsumerStatus(CONSUMER_ID, ConsumerStatus.State.RUNNING, Instant.now());
            when(lifecycleControlPort.status(CONSUMER_ID)).thenReturn(expectedStatus);
            when(lifecycleControlPort.statuses()).thenReturn(List.of(expectedStatus));

            // Act
            service.start(CONSUMER_ID);
            service.pause(CONSUMER_ID);
            service.resume(CONSUMER_ID);
            service.stop(CONSUMER_ID);

            // Assert
            assertThat(service.status(CONSUMER_ID)).isEqualTo(expectedStatus);
            assertThat(service.statuses()).containsExactly(expectedStatus);
            verify(lifecycleControlPort).start(CONSUMER_ID);
            verify(lifecycleControlPort).pause(CONSUMER_ID);
            verify(lifecycleControlPort).resume(CONSUMER_ID);
            verify(lifecycleControlPort).stop(CONSUMER_ID);
        }
    }
}
