package com.template.batchconsumer.domain.model;

import java.time.Instant;

/**
 * @param asOf the instant this status was queried/computed — i.e. when the caller asked, not
 *     the last time the underlying container actually polled Kafka or processed a message. It
 *     carries no operational-freshness information (e.g. "last successful poll time"); it only
 *     timestamps the read itself.
 */
public record ConsumerStatus(String consumerId, State state, Instant asOf) {

    public enum State {
        RUNNING,
        PAUSED,
        STOPPED
    }
}
