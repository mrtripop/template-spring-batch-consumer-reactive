package com.template.batchconsumer.application.metrics;

/** Shared Micrometer metric/tag names, so production code and tests reference one source. */
public final class MetricNames {

    public static final String MESSAGES_PROCESSED = "consumer.messages.processed";
    public static final String PROCESSING_DURATION = "consumer.processing.duration";
    public static final String MESSAGES_RETRIED = "consumer.messages.retried";
    public static final String MESSAGES_DLT = "consumer.messages.dlt";

    public static final String TAG_CONSUMER = "consumer";
    public static final String TAG_OUTCOME = "outcome";

    private MetricNames() {
    }
}
