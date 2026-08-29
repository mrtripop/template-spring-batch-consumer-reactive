package com.template.batchconsumer.domain.model;

public record MessageEnvelope<T>(
        String consumerId,
        String topic,
        int partition,
        long offset,
        String key,
        T payload) {
}
