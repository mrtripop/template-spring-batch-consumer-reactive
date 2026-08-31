package com.template.batchconsumer.config;

import com.template.batchconsumer.application.metrics.MetricNames;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@DisplayName("Kafka consumer config")
class KafkaConsumerConfigTest {

    private final KafkaConsumerConfig config = new KafkaConsumerConfig();

    @Test
    @DisplayName("the consumer factory wraps its value deserializer for poison-pill safety")
    void sampleConsumerFactoryWrapsValueDeserializerForPoisonPillSafety() {
        // Act
        ConsumerFactory<String, ?> consumerFactory =
                config.sampleConsumerFactory("localhost:9092", "sample-consumer");

        // Assert
        assertThat(consumerFactory.getValueDeserializer()).isInstanceOf(ErrorHandlingDeserializer.class);
    }

    // DefaultErrorHandler exposes no public retry-listener accessor in this Spring Kafka version,
    // so this asserts observable behavior (the retry metric increments) instead.
    @Test
    @SuppressWarnings("unchecked")
    @DisplayName("the error handler notifies its retry listener on a retryable failure")
    void errorHandlerNotifiesRetryListenerOnRetryableFailure() {
        // Arrange
        KafkaTemplate<Object, Object> kafkaTemplate = mock(KafkaTemplate.class);
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        DefaultErrorHandler errorHandler = config.kafkaErrorHandler(
                kafkaTemplate, meterRegistry, "sample-consumer", 3, 1000L, 2.0, 10000L);
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("sample-events", 0, 0L, "key", "value");
        Consumer<?, ?> consumer = mock(Consumer.class);
        MessageListenerContainer container = mock(MessageListenerContainer.class);

        // Act
        errorHandler.handleOne(new RuntimeException("boom"), record, consumer, container);

        // Assert
        assertThat(meterRegistry.counter(MetricNames.MESSAGES_RETRIED, MetricNames.TAG_CONSUMER, "sample-consumer").count())
                .isEqualTo(1.0);
    }
}
