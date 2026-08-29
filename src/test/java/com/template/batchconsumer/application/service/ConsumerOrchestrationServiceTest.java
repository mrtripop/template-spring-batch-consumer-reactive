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
}
