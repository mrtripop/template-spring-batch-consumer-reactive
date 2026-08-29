package com.template.batchconsumer.domain.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MessageEnvelopeTest {

    @Test
    void exposesAllFieldsPassedToConstructor() {
        MessageEnvelope<String> envelope = new MessageEnvelope<>(
                "sample-consumer", "sample-events", 2, 42L, "order-1", "payload-body");

        assertThat(envelope.consumerId()).isEqualTo("sample-consumer");
        assertThat(envelope.topic()).isEqualTo("sample-events");
        assertThat(envelope.partition()).isEqualTo(2);
        assertThat(envelope.offset()).isEqualTo(42L);
        assertThat(envelope.key()).isEqualTo("order-1");
        assertThat(envelope.payload()).isEqualTo("payload-body");
    }

    @Test
    void twoEnvelopesWithSameFieldsAreEqual() {
        MessageEnvelope<String> first = new MessageEnvelope<>("c1", "t1", 0, 1L, "k1", "p1");
        MessageEnvelope<String> second = new MessageEnvelope<>("c1", "t1", 0, 1L, "k1", "p1");

        assertThat(first).isEqualTo(second);
        assertThat(first.hashCode()).isEqualTo(second.hashCode());
    }
}
