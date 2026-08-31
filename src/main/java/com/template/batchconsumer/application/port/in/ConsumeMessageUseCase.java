package com.template.batchconsumer.application.port.in;

import com.template.batchconsumer.domain.model.MessageEnvelope;
import com.template.batchconsumer.domain.model.ProcessingOutcome;
import reactor.core.publisher.Mono;

public interface ConsumeMessageUseCase<T> {

    Mono<ProcessingOutcome> consume(MessageEnvelope<T> envelope);
}
