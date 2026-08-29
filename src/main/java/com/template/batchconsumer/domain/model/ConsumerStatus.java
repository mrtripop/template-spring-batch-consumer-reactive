package com.template.batchconsumer.domain.model;

import java.time.Instant;

public record ConsumerStatus(String consumerId, State state, Instant asOf) {

    public enum State {
        RUNNING,
        PAUSED,
        STOPPED
    }
}
