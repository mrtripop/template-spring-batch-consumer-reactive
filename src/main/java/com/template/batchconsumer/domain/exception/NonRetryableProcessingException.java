package com.template.batchconsumer.domain.exception;

public class NonRetryableProcessingException extends RuntimeException {

    public NonRetryableProcessingException(String message) {
        super(message);
    }

    public NonRetryableProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}
