package com.template.batchconsumer.config;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class KafkaConsumerConfigTest {

    private final KafkaConsumerConfig config = new KafkaConsumerConfig();

    /**
     * The brief's original assertion ({@code errorHandler.retryListeners()}) does not compile
     * against the Spring Kafka version resolved in this environment (4.1.1): {@code
     * DefaultErrorHandler} exposes no public retry-listener accessor (only a {@code protected
     * getRetryListeners()} on its {@code FailedBatchProcessor} superclass, in a different
     * package). Per the brief's own fallback guidance, this test instead asserts on observable
     * behavior: triggering a retryable failure through the error handler and checking that the
     * {@code consumer.messages.retried} counter registered by the retry listener increments.
     */
    @Test
    @SuppressWarnings("unchecked")
    void errorHandlerNotifiesRetryListenerOnRetryableFailure() {
        KafkaTemplate<Object, Object> kafkaTemplate = mock(KafkaTemplate.class);
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

        DefaultErrorHandler errorHandler = config.kafkaErrorHandler(
                kafkaTemplate, meterRegistry, "sample-consumer", 3, 1000L, 2.0, 10000L);

        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("sample-events", 0, 0L, "key", "value");
        Consumer<?, ?> consumer = mock(Consumer.class);
        MessageListenerContainer container = mock(MessageListenerContainer.class);

        errorHandler.handleOne(new RuntimeException("boom"), record, consumer, container);

        assertThat(meterRegistry.counter("consumer.messages.retried", "consumer", "sample-consumer").count())
                .isEqualTo(1.0);
    }
}
