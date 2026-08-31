package com.template.batchconsumer.config;

import com.template.batchconsumer.application.metrics.MetricNames;
import com.template.batchconsumer.application.sample.SamplePayload;
import com.template.batchconsumer.domain.exception.NonRetryableProcessingException;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.HashMap;
import java.util.Map;

// This project has no Boot Kafka starter, so @KafkaListener processing needs enabling explicitly.
@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, SamplePayload> sampleConsumerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${consumer.sample.id}") String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        JacksonJsonDeserializer<SamplePayload> valueDeserializer = new JacksonJsonDeserializer<>(SamplePayload.class);
        valueDeserializer.addTrustedPackages(SamplePayload.class.getPackageName());

        // Wraps the Jackson deserializer so a malformed ("poison pill") record surfaces through
        // the normal error-handler/DLT path instead of wedging the consumer on that offset.
        ErrorHandlingDeserializer<SamplePayload> errorHandlingValueDeserializer =
                new ErrorHandlingDeserializer<>(valueDeserializer);

        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), errorHandlingValueDeserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, SamplePayload> sampleKafkaListenerContainerFactory(
            ConsumerFactory<String, SamplePayload> sampleConsumerFactory,
            DefaultErrorHandler kafkaErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, SamplePayload> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(sampleConsumerFactory);
        factory.setCommonErrorHandler(kafkaErrorHandler);
        return factory;
    }

    // No Boot Kafka autoconfiguration in this project, so the DLT-publishing template used by
    // kafkaErrorHandler below is wired manually.
    @Bean
    public ProducerFactory<Object, Object> kafkaProducerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<Object, Object> kafkaTemplate(ProducerFactory<Object, Object> kafkaProducerFactory) {
        return new KafkaTemplate<>(kafkaProducerFactory);
    }

    // Retry/DLT metrics are tagged with this bean's own consumerId — a second @KafkaListener
    // needs its own kafkaErrorHandler-style bean, or its events get mislabeled under this one.
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(
            KafkaTemplate<Object, Object> kafkaTemplate,
            MeterRegistry meterRegistry,
            @Value("${consumer.sample.id}") String consumerId,
            @Value("${consumer.sample.retry.max-attempts}") int maxAttempts,
            @Value("${consumer.sample.retry.initial-interval-ms}") long initialIntervalMs,
            @Value("${consumer.sample.retry.multiplier}") double multiplier,
            @Value("${consumer.sample.retry.max-interval-ms}") long maxIntervalMs) {

        ExponentialBackOff backOff = new ExponentialBackOff();
        backOff.setMaxAttempts(maxAttempts);
        backOff.setInitialInterval(initialIntervalMs);
        backOff.setMultiplier(multiplier);
        backOff.setMaxInterval(maxIntervalMs);

        DeadLetterPublishingRecoverer dltRecoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
        ConsumerRecordRecoverer countingRecoverer = (record, exception) -> {
            meterRegistry.counter(MetricNames.MESSAGES_DLT, MetricNames.TAG_CONSUMER, consumerId).increment();
            dltRecoverer.accept(record, exception);
        };

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(countingRecoverer, backOff);
        errorHandler.addNotRetryableExceptions(NonRetryableProcessingException.class);
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) ->
                meterRegistry.counter(MetricNames.MESSAGES_RETRIED, MetricNames.TAG_CONSUMER, consumerId).increment());
        return errorHandler;
    }
}
