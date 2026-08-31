package com.template.batchconsumer.application.sample;

import com.template.batchconsumer.domain.exception.NonRetryableProcessingException;
import com.template.batchconsumer.domain.exception.RetryableProcessingException;
import com.template.batchconsumer.domain.model.MessageEnvelope;
import com.template.batchconsumer.domain.model.ProcessingOutcome;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

@DisplayName("Sample message processor")
class SampleMessageProcessorTest {

    private static final String CONSUMER_ID = "sample-consumer";
    private static final String TOPIC = "sample-events";

    private final SampleMessageProcessor processor = new SampleMessageProcessor();

    @Test
    @DisplayName("an ordinary payload succeeds")
    void succeedsForOrdinaryPayload() {
        // Arrange
        MessageEnvelope<SamplePayload> envelope = envelopeWithMessage("hello world");

        // Act & Assert
        StepVerifier.create(processor.process(envelope))
                .expectNext(ProcessingOutcome.SUCCESS)
                .verifyComplete();
    }

    @Test
    @DisplayName("the retryable trigger raises a RetryableProcessingException")
    void raisesRetryableExceptionForRetryTrigger() {
        // Arrange
        MessageEnvelope<SamplePayload> envelope = envelopeWithMessage(SampleMessageProcessor.RETRYABLE_TRIGGER);

        // Act & Assert
        StepVerifier.create(processor.process(envelope))
                .expectError(RetryableProcessingException.class)
                .verify();
    }

    @Test
    @DisplayName("the non-retryable trigger raises a NonRetryableProcessingException")
    void raisesNonRetryableExceptionForFatalTrigger() {
        // Arrange
        MessageEnvelope<SamplePayload> envelope = envelopeWithMessage(SampleMessageProcessor.NON_RETRYABLE_TRIGGER);

        // Act & Assert
        StepVerifier.create(processor.process(envelope))
                .expectError(NonRetryableProcessingException.class)
                .verify();
    }

    private MessageEnvelope<SamplePayload> envelopeWithMessage(String message) {
        return new MessageEnvelope<>(CONSUMER_ID, TOPIC, 0, 0L, "key", new SamplePayload("id-1", message));
    }
}
