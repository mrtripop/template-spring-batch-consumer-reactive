package com.template.batchconsumer.application.port.out;

import com.template.batchconsumer.domain.model.ConsumerStatus;

import java.util.List;

public interface ConsumerLifecycleControlPort {

    void start(String consumerId);

    void stop(String consumerId);

    void pause(String consumerId);

    void resume(String consumerId);

    ConsumerStatus status(String consumerId);

    List<ConsumerStatus> statuses();
}
