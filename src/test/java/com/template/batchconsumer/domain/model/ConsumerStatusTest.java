package com.template.batchconsumer.domain.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Consumer status")
class ConsumerStatusTest {

    @Test
    @DisplayName("exposes all fields passed to the constructor")
    void exposesAllFieldsPassedToConstructor() {
        // Arrange & Act
        Instant now = Instant.now();
        ConsumerStatus status = new ConsumerStatus("sample-consumer", ConsumerStatus.State.RUNNING, now);

        // Assert
        assertThat(status.consumerId()).isEqualTo("sample-consumer");
        assertThat(status.state()).isEqualTo(ConsumerStatus.State.RUNNING);
        assertThat(status.asOf()).isEqualTo(now);
    }
}
