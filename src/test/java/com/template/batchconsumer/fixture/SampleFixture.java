package com.template.batchconsumer.fixture;

import com.template.batchconsumer.application.sample.SamplePayload;
import com.template.batchconsumer.config.SampleConsumerProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/** Shared sample-consumer test data — one source of truth for setup and assertions. */
public final class SampleFixture {

    public static final String CONSUMER_ID = "sample-consumer";
    public static final String UNKNOWN_CONSUMER_ID = "does-not-exist";
    public static final String TOPIC = "sample-events";
    public static final String DLT_TOPIC = "sample-events-dlt";
    public static final long BLOCK_TIMEOUT_MS = 30_000L;

    private SampleFixture() {
    }

    public static SamplePayload samplePayload(String id, String message) {
        return new SamplePayload(id, message);
    }

    public static ConsumerRecord<String, SamplePayload> consumerRecord(
            int partition, long offset, String key, SamplePayload payload) {
        return new ConsumerRecord<>(TOPIC, partition, offset, key, payload);
    }

    public static SampleConsumerProperties consumerProperties() {
        return new SampleConsumerProperties(
                CONSUMER_ID, TOPIC, 3, BLOCK_TIMEOUT_MS, "earliest",
                new SampleConsumerProperties.Retry(3, 1000L, 2.0, 10000L));
    }
}
