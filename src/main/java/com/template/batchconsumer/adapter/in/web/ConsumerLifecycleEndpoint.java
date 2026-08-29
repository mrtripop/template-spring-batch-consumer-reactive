package com.template.batchconsumer.adapter.in.web;

import com.template.batchconsumer.application.port.in.ConsumerLifecycleUseCase;
import com.template.batchconsumer.domain.model.ConsumerStatus;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;
import org.springframework.boot.actuate.endpoint.web.WebEndpointResponse;

import java.util.List;
import java.util.NoSuchElementException;

@Endpoint(id = "consumers")
public class ConsumerLifecycleEndpoint {

    private final ConsumerLifecycleUseCase consumerLifecycleUseCase;

    public ConsumerLifecycleEndpoint(ConsumerLifecycleUseCase consumerLifecycleUseCase) {
        this.consumerLifecycleUseCase = consumerLifecycleUseCase;
    }

    @ReadOperation
    public List<ConsumerStatus> statuses() {
        return consumerLifecycleUseCase.statuses();
    }

    // Translates NoSuchElementException (thrown by KafkaListenerLifecycleAdapter.containerFor
    // when the id doesn't match any registered listener container) to HTTP 404 here, at the
    // adapter.in.web boundary — the appropriate place for HTTP-status translation, so that
    // adapter.out.kafka can stay Spring-Kafka-specific rather than growing a web-layer concern.
    @ReadOperation
    public WebEndpointResponse<ConsumerStatus> status(@Selector String consumerId) {
        try {
            return new WebEndpointResponse<>(consumerLifecycleUseCase.status(consumerId));
        } catch (NoSuchElementException ex) {
            return new WebEndpointResponse<>(WebEndpointResponse.STATUS_NOT_FOUND);
        }
    }

    // See the note on status(String) above: same 404 translation for the write path so a
    // typo'd/unknown consumer id reads as "not found" rather than a generic 500 server error.
    @WriteOperation
    public WebEndpointResponse<Void> applyAction(@Selector String consumerId, @Selector Action action) {
        try {
            switch (action) {
                case PAUSE -> consumerLifecycleUseCase.pause(consumerId);
                case RESUME -> consumerLifecycleUseCase.resume(consumerId);
                case STOP -> consumerLifecycleUseCase.stop(consumerId);
                case START -> consumerLifecycleUseCase.start(consumerId);
            }
            return new WebEndpointResponse<>(WebEndpointResponse.STATUS_OK);
        } catch (NoSuchElementException ex) {
            return new WebEndpointResponse<>(WebEndpointResponse.STATUS_NOT_FOUND);
        }
    }

    public enum Action {
        PAUSE, RESUME, STOP, START
    }
}
