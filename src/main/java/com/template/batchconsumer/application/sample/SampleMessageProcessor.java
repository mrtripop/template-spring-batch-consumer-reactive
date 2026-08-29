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
