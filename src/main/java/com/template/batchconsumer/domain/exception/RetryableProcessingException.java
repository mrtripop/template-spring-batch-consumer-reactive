package com.template.batchconsumer.domain.exception;

public class RetryableProcessingException extends RuntimeException {

    public RetryableProcessingException(String message) {
        super(message);
    }

    public RetryableProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}
