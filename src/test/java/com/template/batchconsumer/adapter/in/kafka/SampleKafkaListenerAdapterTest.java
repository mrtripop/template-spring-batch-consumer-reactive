package com.template.batchconsumer.adapter.in.kafka;

import com.template.batchconsumer.application.port.in.ConsumeMessageUseCase;
import com.template.batchconsumer.application.sample.SamplePayload;
import com.template.batchconsumer.domain.model.MessageEnvelope;
import com.template.batchconsumer.domain.model.ProcessingOutcome;
import com.template.batchconsumer.fixture.SampleFixture;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Sample Kafka listener adapter")
class SampleKafkaListenerAdapterTest {

    @SuppressWarnings("unchecked")
    private final ConsumeMessageUseCase<SamplePayload> consumeMessageUseCase = Mockito.mock(ConsumeMessageUseCase.class);
    private final SampleKafkaListenerAdapter adapter = new SampleKafkaListenerAdapter(
            consumeMessageUseCase, SampleFixture.CONSUMER_ID, SampleFixture.BLOCK_TIMEOUT_MS);

    @Test
    @DisplayName("converts a ConsumerRecord into a MessageEnvelope, copying every field")
    void toEnvelopeCopiesAllRecordFields() {
        // Arrange
        int partition = 2;
        long offset = 42L;
        String key = "key-1";
        SamplePayload payload = SampleFixture.samplePayload("id-1", "hello");
        ConsumerRecord<String, SamplePayload> record = SampleFixture.consumerRecord(partition, offset, key, payload);

        // Act
        MessageEnvelope<SamplePayload> envelope = adapter.toEnvelope(record);

        // Assert
        assertThat(envelope.consumerId()).isEqualTo(SampleFixture.CONSUMER_ID);
        assertThat(envelope.topic()).isEqualTo(SampleFixture.TOPIC);
        assertThat(envelope.partition()).isEqualTo(partition);
        assertThat(envelope.offset()).isEqualTo(offset);
        assertThat(envelope.key()).isEqualTo(key);
        assertThat(envelope.payload()).isEqualTo(payload);
    }

    @Test
    @DisplayName("bridges onMessage by blocking on the composed use-case Mono")
    void onMessageBlocksOnComposedUseCaseMono() {
        // Arrange
        SamplePayload payload = SampleFixture.samplePayload("id-2", "hello");
        ConsumerRecord<String, SamplePayload> record = SampleFixture.consumerRecord(0, 0L, "key-2", payload);
        Mockito.when(consumeMessageUseCase.consume(Mockito.any()))
                .thenReturn(Mono.just(ProcessingOutcome.SUCCESS));

        // Act
        adapter.onMessage(record);

        // Assert
        Mockito.verify(consumeMessageUseCase).consume(adapter.toEnvelope(record));
    }
}
