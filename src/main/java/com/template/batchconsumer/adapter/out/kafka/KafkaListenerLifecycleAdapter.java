package com.template.batchconsumer.adapter.out.kafka;

import com.template.batchconsumer.application.port.out.ConsumerLifecycleControlPort;
import com.template.batchconsumer.domain.model.ConsumerStatus;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.time.Instant;
import java.util.List;
import java.util.NoSuchElementException;

public class KafkaListenerLifecycleAdapter implements ConsumerLifecycleControlPort {

    private final KafkaListenerEndpointRegistry registry;

    public KafkaListenerLifecycleAdapter(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void start(String consumerId) {
        containerFor(consumerId).start();
    }

    @Override
    public void stop(String consumerId) {
        containerFor(consumerId).stop();
    }

    @Override
    public void pause(String consumerId) {
        containerFor(consumerId).pause();
    }

    @Override
    public void resume(String consumerId) {
        containerFor(consumerId).resume();
    }

    @Override
    public ConsumerStatus status(String consumerId) {
        return toStatus(consumerId, containerFor(consumerId));
    }

    @Override
    public List<ConsumerStatus> statuses() {
        return registry.getListenerContainerIds().stream()
                .sorted()
                .map(id -> toStatus(id, registry.getListenerContainer(id)))
                .toList();
    }

    private MessageListenerContainer containerFor(String consumerId) {
        MessageListenerContainer container = registry.getListenerContainer(consumerId);
        if (container == null) {
            throw new NoSuchElementException("No Kafka listener container registered with id " + consumerId);
        }
        return container;
    }

    private ConsumerStatus toStatus(String consumerId, MessageListenerContainer container) {
        ConsumerStatus.State state;
        if (!container.isRunning()) {
            state = ConsumerStatus.State.STOPPED;
        } else if (container.isContainerPaused()) {
            state = ConsumerStatus.State.PAUSED;
        } else {
            state = ConsumerStatus.State.RUNNING;
        }
        return new ConsumerStatus(consumerId, state, Instant.now());
    }
}
