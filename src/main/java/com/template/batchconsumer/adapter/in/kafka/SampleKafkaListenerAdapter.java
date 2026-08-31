package com.template.batchconsumer.adapter.in.kafka;

import com.template.batchconsumer.application.port.in.ConsumeMessageUseCase;
import com.template.batchconsumer.application.sample.SamplePayload;
import com.template.batchconsumer.domain.model.MessageEnvelope;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Example Kafka listener adapter — bridges Spring Kafka's imperative {@code @KafkaListener}
 * delivery model into the reactive {@link ConsumeMessageUseCase}. Delete this class (and
 * {@code SamplePayload}/{@code SampleMessageProcessor} from the application.sample package)
 * when adopting the template for a real consumer.
 */
@Component
public class SampleKafkaListenerAdapter {

    private final ConsumeMessageUseCase<SamplePayload> consumeMessageUseCase;
    private final String consumerId;
    private final Duration blockTimeout;

    public SampleKafkaListenerAdapter(
            ConsumeMessageUseCase<SamplePayload> consumeMessageUseCase,
            @Value("${consumer.sample.id}") String consumerId,
            @Value("${consumer.sample.block-timeout-ms:30000}") long blockTimeoutMs) {
        this.consumeMessageUseCase = consumeMessageUseCase;
        this.consumerId = consumerId;
        this.blockTimeout = Duration.ofMillis(blockTimeoutMs);
    }

    @KafkaListener(
            id = "${consumer.sample.id}",
            topics = "${consumer.sample.topic}",
            concurrency = "${consumer.sample.concurrency}",
            containerFactory = "sampleKafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, SamplePayload> record) {
        // Single, outermost blocking bridge point (spec Decision Log #3).
        consumeMessageUseCase.consume(toEnvelope(record)).block(blockTimeout);
    }

    MessageEnvelope<SamplePayload> toEnvelope(ConsumerRecord<String, SamplePayload> record) {
        return new MessageEnvelope<>(
                consumerId,
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value());
    }
}
