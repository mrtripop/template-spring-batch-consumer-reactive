package com.template.batchconsumer.adapter.in.web;

import com.template.batchconsumer.application.port.in.ConsumerLifecycleUseCase;
import com.template.batchconsumer.domain.model.ConsumerStatus;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;

import java.util.List;

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

    @ReadOperation
    public ConsumerStatus status(@Selector String consumerId) {
        return consumerLifecycleUseCase.status(consumerId);
    }

    @WriteOperation
    public void applyAction(@Selector String consumerId, @Selector Action action) {
        switch (action) {
            case PAUSE -> consumerLifecycleUseCase.pause(consumerId);
            case RESUME -> consumerLifecycleUseCase.resume(consumerId);
            case STOP -> consumerLifecycleUseCase.stop(consumerId);
            case START -> consumerLifecycleUseCase.start(consumerId);
        }
    }

    public enum Action {
        PAUSE, RESUME, STOP, START
    }
}
