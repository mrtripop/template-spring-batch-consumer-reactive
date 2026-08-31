package com.template.batchconsumer.domain.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Message envelope")
class MessageEnvelopeTest {

    @Test
    @DisplayName("exposes all fields passed to the constructor")
    void exposesAllFieldsPassedToConstructor() {
        // Arrange & Act
        MessageEnvelope<String> envelope = new MessageEnvelope<>(
                "sample-consumer", "sample-events", 2, 42L, "order-1", "payload-body");

        // Assert
        assertThat(envelope.consumerId()).isEqualTo("sample-consumer");
        assertThat(envelope.topic()).isEqualTo("sample-events");
        assertThat(envelope.partition()).isEqualTo(2);
        assertThat(envelope.offset()).isEqualTo(42L);
        assertThat(envelope.key()).isEqualTo("order-1");
        assertThat(envelope.payload()).isEqualTo("payload-body");
    }

    @Test
    @DisplayName("two envelopes with the same fields are equal")
    void twoEnvelopesWithSameFieldsAreEqual() {
        // Arrange & Act
        MessageEnvelope<String> first = new MessageEnvelope<>("c1", "t1", 0, 1L, "k1", "p1");
        MessageEnvelope<String> second = new MessageEnvelope<>("c1", "t1", 0, 1L, "k1", "p1");

        // Assert
        assertThat(first).isEqualTo(second);
        assertThat(first.hashCode()).isEqualTo(second.hashCode());
    }
}
