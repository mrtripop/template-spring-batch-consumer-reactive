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

    private MessageEnvelope<SamplePayload> envelopeWithMessage(String message) {
        return new MessageEnvelope<>("sample-consumer", "sample-events", 0, 0L, "key", new SamplePayload("id-1", message));
    }
}
