package com.template.batchconsumer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "consumer.sample")
public record SampleConsumerProperties(
        String id,
        String topic,
        int concurrency,
        @DefaultValue("30000") long blockTimeoutMs,
        @DefaultValue("earliest") String autoOffsetReset,
        Retry retry) {

    public record Retry(
            int maxAttempts,
            @DefaultValue("1000") long initialIntervalMs,
            @DefaultValue("2.0") double multiplier,
            @DefaultValue("10000") long maxIntervalMs) {
    }
}
