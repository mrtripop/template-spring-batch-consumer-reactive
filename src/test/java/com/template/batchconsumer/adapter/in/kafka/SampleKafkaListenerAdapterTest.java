package com.template.batchconsumer.adapter.in.kafka;

import com.template.batchconsumer.application.port.in.ConsumeMessageUseCase;
import com.template.batchconsumer.application.sample.SamplePayload;
import com.template.batchconsumer.domain.model.MessageEnvelope;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

class SampleKafkaListenerAdapterTest {

    @SuppressWarnings("unchecked")
    private final ConsumeMessageUseCase<SamplePayload> consumeMessageUseCase = Mockito.mock(ConsumeMessageUseCase.class);
    private final SampleKafkaListenerAdapter adapter =
            new SampleKafkaListenerAdapter(consumeMessageUseCase, "sample-consumer");

    @Test
    void toEnvelopeCopiesAllRecordFields() {
        SamplePayload payload = new SamplePayload("id-1", "hello");
        ConsumerRecord<String, SamplePayload> record =
                new ConsumerRecord<>("sample-events", 2, 42L, "key-1", payload);

        MessageEnvelope<SamplePayload> envelope = adapter.toEnvelope(record);

        assertThat(envelope.consumerId()).isEqualTo("sample-consumer");
        assertThat(envelope.topic()).isEqualTo("sample-events");
        assertThat(envelope.partition()).isEqualTo(2);
        assertThat(envelope.offset()).isEqualTo(42L);
        assertThat(envelope.key()).isEqualTo("key-1");
        assertThat(envelope.payload()).isEqualTo(payload);
    }

    @Test
    void onMessageBlocksOnComposedUseCaseMono() {
        SamplePayload payload = new SamplePayload("id-2", "hello");
        ConsumerRecord<String, SamplePayload> record =
                new ConsumerRecord<>("sample-events", 0, 0L, "key-2", payload);
        Mockito.when(consumeMessageUseCase.consume(Mockito.any()))
                .thenReturn(reactor.core.publisher.Mono.just(com.template.batchconsumer.domain.model.ProcessingOutcome.SUCCESS));

        adapter.onMessage(record);

        Mockito.verify(consumeMessageUseCase).consume(adapter.toEnvelope(record));
    }
}
