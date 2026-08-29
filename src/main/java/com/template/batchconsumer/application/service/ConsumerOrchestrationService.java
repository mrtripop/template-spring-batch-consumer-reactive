package com.template.batchconsumer.application.service;

import com.template.batchconsumer.application.port.in.ConsumeMessageUseCase;
import com.template.batchconsumer.application.port.in.ConsumerLifecycleUseCase;
import com.template.batchconsumer.application.port.in.MessageProcessor;
import com.template.batchconsumer.application.port.out.ConsumerLifecycleControlPort;
import com.template.batchconsumer.domain.exception.NonRetryableProcessingException;
import com.template.batchconsumer.domain.model.ConsumerStatus;
import com.template.batchconsumer.domain.model.MessageEnvelope;
import com.template.batchconsumer.domain.model.ProcessingOutcome;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import reactor.core.publisher.Mono;

import java.util.List;

public class ConsumerOrchestrationService<T> implements ConsumeMessageUseCase<T>, ConsumerLifecycleUseCase {

    private final String consumerId;
    private final MessageProcessor<T> messageProcessor;
    private final MeterRegistry meterRegistry;
    private final ConsumerLifecycleControlPort lifecycleControlPort;

    public ConsumerOrchestrationService(
            String consumerId,
            MessageProcessor<T> messageProcessor,
            MeterRegistry meterRegistry,
            ConsumerLifecycleControlPort lifecycleControlPort) {
        this.consumerId = consumerId;
        this.messageProcessor = messageProcessor;
        this.meterRegistry = meterRegistry;
        this.lifecycleControlPort = lifecycleControlPort;
    }

    @Override
    public Mono<ProcessingOutcome> consume(MessageEnvelope<T> envelope) {
        Timer.Sample sample = Timer.start(meterRegistry);
        return messageProcessor.process(envelope)
                .doOnNext(outcome -> recordSuccessOutcome(outcome, sample))
                .doOnError(error -> recordErrorOutcome(error, sample));
    }

    private void recordSuccessOutcome(ProcessingOutcome outcome, Timer.Sample sample) {
        meterRegistry.counter("consumer.messages.processed", "consumer", consumerId, "outcome", outcome.name())
                .increment();
        sample.stop(meterRegistry.timer("consumer.processing.duration", "consumer", consumerId));
    }

    private void recordErrorOutcome(Throwable error, Timer.Sample sample) {
        ProcessingOutcome outcome = error instanceof NonRetryableProcessingException
                ? ProcessingOutcome.NON_RETRYABLE_FAILURE
                : ProcessingOutcome.RETRYABLE_FAILURE;
        meterRegistry.counter("consumer.messages.processed", "consumer", consumerId, "outcome", outcome.name())
                .increment();
        sample.stop(meterRegistry.timer("consumer.processing.duration", "consumer", consumerId));
    }

    @Override
    public void start(String targetConsumerId) {
        lifecycleControlPort.start(targetConsumerId);
    }

    @Override
    public void stop(String targetConsumerId) {
        lifecycleControlPort.stop(targetConsumerId);
    }

    @Override
    public void pause(String targetConsumerId) {
        lifecycleControlPort.pause(targetConsumerId);
    }

    @Override
    public void resume(String targetConsumerId) {
        lifecycleControlPort.resume(targetConsumerId);
    }

    @Override
    public ConsumerStatus status(String targetConsumerId) {
        return lifecycleControlPort.status(targetConsumerId);
    }

    @Override
    public List<ConsumerStatus> statuses() {
        return lifecycleControlPort.statuses();
    }
}
