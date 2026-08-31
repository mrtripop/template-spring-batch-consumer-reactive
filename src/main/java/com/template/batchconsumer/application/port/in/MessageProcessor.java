package com.template.batchconsumer.application.port.in;

import com.template.batchconsumer.domain.model.MessageEnvelope;
import com.template.batchconsumer.domain.model.ProcessingOutcome;
import reactor.core.publisher.Mono;

public interface MessageProcessor<T> {

    Mono<ProcessingOutcome> process(MessageEnvelope<T> envelope);
}
