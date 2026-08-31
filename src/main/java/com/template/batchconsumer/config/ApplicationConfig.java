package com.template.batchconsumer.config;

import com.template.batchconsumer.adapter.out.kafka.KafkaListenerLifecycleAdapter;
import com.template.batchconsumer.application.port.in.ConsumeMessageUseCase;
import com.template.batchconsumer.application.port.in.ConsumerLifecycleUseCase;
import com.template.batchconsumer.application.port.out.ConsumerLifecycleControlPort;
import com.template.batchconsumer.application.sample.SampleMessageProcessor;
import com.template.batchconsumer.application.sample.SamplePayload;
import com.template.batchconsumer.application.service.ConsumerOrchestrationService;
import com.template.batchconsumer.adapter.in.web.ConsumerLifecycleEndpoint;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

@Configuration
public class ApplicationConfig {

    @Bean
    public ConsumerLifecycleControlPort consumerLifecycleControlPort(KafkaListenerEndpointRegistry registry) {
        return new KafkaListenerLifecycleAdapter(registry);
    }

    @Bean
    public ConsumerOrchestrationService<SamplePayload> sampleConsumerOrchestrationService(
            SampleConsumerProperties properties,
            MeterRegistry meterRegistry,
            ConsumerLifecycleControlPort consumerLifecycleControlPort) {
        return new ConsumerOrchestrationService<>(
                properties.id(), new SampleMessageProcessor(), meterRegistry, consumerLifecycleControlPort);
    }

    // @Primary breaks a real ambiguity: ConsumerOrchestrationService implements both use-case
    // interfaces directly, so it's also an autowire candidate here. Only this alias needs it —
    // consumerLifecycleUseCase below resolves by name-match instead.
    @Bean
    @Primary
    public ConsumeMessageUseCase<SamplePayload> sampleConsumeMessageUseCase(
            ConsumerOrchestrationService<SamplePayload> sampleConsumerOrchestrationService) {
        return sampleConsumerOrchestrationService;
    }

    @Bean
    public ConsumerLifecycleUseCase consumerLifecycleUseCase(
            ConsumerOrchestrationService<SamplePayload> sampleConsumerOrchestrationService) {
        return sampleConsumerOrchestrationService;
    }

    @Bean
    public ConsumerLifecycleEndpoint consumerLifecycleEndpoint(ConsumerLifecycleUseCase consumerLifecycleUseCase) {
        return new ConsumerLifecycleEndpoint(consumerLifecycleUseCase);
    }
}
