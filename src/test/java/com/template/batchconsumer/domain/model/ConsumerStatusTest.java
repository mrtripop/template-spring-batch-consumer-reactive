package com.template.batchconsumer.domain.model;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class ConsumerStatusTest {

    @Test
    void exposesAllFieldsPassedToConstructor() {
        Instant now = Instant.now();
        ConsumerStatus status = new ConsumerStatus("sample-consumer", ConsumerStatus.State.RUNNING, now);

        assertThat(status.consumerId()).isEqualTo("sample-consumer");
        assertThat(status.state()).isEqualTo(ConsumerStatus.State.RUNNING);
        assertThat(status.asOf()).isEqualTo(now);
    }
}
