package com.template.batchconsumer.config;

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
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.HashMap;
import java.util.Map;

// @EnableKafka: in this environment's resolved Spring Boot version, spring-boot-autoconfigure
// ships no Kafka autoconfiguration at all (the project depends on the bare spring-kafka
// artifact rather than a Boot Kafka starter), so annotation-driven @KafkaListener processing —
// and the KafkaListenerEndpointRegistry that KafkaListenerLifecycleAdapter depends on — must be
// enabled explicitly here rather than relying on Boot to wire it in automatically.
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

        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), valueDeserializer);
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

    // ProducerFactory/KafkaTemplate: in this environment's resolved Spring Boot version, Boot's
    // Kafka autoconfiguration (which would normally supply a default KafkaTemplate<Object,
    // Object> bean from spring.kafka.* properties) does not exist at all, so the DLT-publishing
    // template used by kafkaErrorHandler below must be wired here instead.
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
            meterRegistry.counter("consumer.messages.dlt", "consumer", consumerId).increment();
            dltRecoverer.accept(record, exception);
        };

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(countingRecoverer, backOff);
        errorHandler.addNotRetryableExceptions(NonRetryableProcessingException.class);
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) ->
                meterRegistry.counter("consumer.messages.retried", "consumer", consumerId).increment());
        return errorHandler;
    }
}
