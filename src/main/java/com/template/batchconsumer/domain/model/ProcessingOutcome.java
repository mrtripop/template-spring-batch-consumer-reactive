package com.template.batchconsumer.domain.model;

public enum ProcessingOutcome {
    SUCCESS,
    RETRYABLE_FAILURE,
    NON_RETRYABLE_FAILURE
}
