package com.template.batchconsumer.adapter.in.kafka;

import com.template.batchconsumer.application.port.in.ConsumeMessageUseCase;
import com.template.batchconsumer.application.sample.SamplePayload;
import com.template.batchconsumer.domain.model.MessageEnvelope;
import com.template.batchconsumer.domain.model.ProcessingOutcome;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Sample Kafka listener adapter")
class SampleKafkaListenerAdapterTest {

    private static final long BLOCK_TIMEOUT_MS = 30_000L;

    @SuppressWarnings("unchecked")
    private final ConsumeMessageUseCase<SamplePayload> consumeMessageUseCase = Mockito.mock(ConsumeMessageUseCase.class);
    private final SampleKafkaListenerAdapter adapter =
            new SampleKafkaListenerAdapter(consumeMessageUseCase, "sample-consumer", BLOCK_TIMEOUT_MS);

    @Test
    @DisplayName("converts a ConsumerRecord into a MessageEnvelope, copying every field")
    void toEnvelopeCopiesAllRecordFields() {
        // Arrange
        SamplePayload payload = new SamplePayload("id-1", "hello");
        ConsumerRecord<String, SamplePayload> record =
                new ConsumerRecord<>("sample-events", 2, 42L, "key-1", payload);

        // Act
        MessageEnvelope<SamplePayload> envelope = adapter.toEnvelope(record);

        // Assert
        assertThat(envelope.consumerId()).isEqualTo("sample-consumer");
        assertThat(envelope.topic()).isEqualTo("sample-events");
        assertThat(envelope.partition()).isEqualTo(2);
        assertThat(envelope.offset()).isEqualTo(42L);
        assertThat(envelope.key()).isEqualTo("key-1");
        assertThat(envelope.payload()).isEqualTo(payload);
    }

    @Test
    @DisplayName("bridges onMessage by blocking on the composed use-case Mono")
    void onMessageBlocksOnComposedUseCaseMono() {
        // Arrange
        SamplePayload payload = new SamplePayload("id-2", "hello");
        ConsumerRecord<String, SamplePayload> record =
                new ConsumerRecord<>("sample-events", 0, 0L, "key-2", payload);
        Mockito.when(consumeMessageUseCase.consume(Mockito.any()))
                .thenReturn(Mono.just(ProcessingOutcome.SUCCESS));

        // Act
        adapter.onMessage(record);

        // Assert
        Mockito.verify(consumeMessageUseCase).consume(adapter.toEnvelope(record));
    }
}
