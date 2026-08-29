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
import org.springframework.beans.factory.annotation.Value;
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
            @Value("${consumer.sample.id}") String consumerId,
            MeterRegistry meterRegistry,
            ConsumerLifecycleControlPort consumerLifecycleControlPort) {
        return new ConsumerOrchestrationService<>(
                consumerId, new SampleMessageProcessor(), meterRegistry, consumerLifecycleControlPort);
    }

    // @Primary: ConsumerOrchestrationService implements both ConsumeMessageUseCase and
    // ConsumerLifecycleUseCase directly, so it is itself an autowire candidate for either
    // interface. Without @Primary here, any injection point that autowires by interface type
    // alone (e.g. SampleKafkaListenerAdapter's ConsumeMessageUseCase<SamplePayload> constructor
    // parameter, whose parameter name doesn't match either bean name) is ambiguous between the
    // orchestration service bean and this narrower port-typed alias.
    //
    // Only ONE of the two port-typed alias beans below is marked @Primary. Both ultimately
    // return the very same singleton instance, so once that instance is created, Spring type-
    // matches it as a candidate under any of its three bean names for either interface. Marking
    // both @Primary therefore creates a *second* ambiguity ("more than one primary bean found")
    // for whichever interface type they both satisfy. With a single @Primary bean, it resolves
    // unambiguously for both ConsumeMessageUseCase and ConsumerLifecycleUseCase injection points;
    // consumerLifecycleUseCase below still resolves correctly for its own consumer
    // (ConsumerLifecycleEndpoint) via that bean's own name-matching parameter.
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
