package com.ms.silverking.util;

/**
 * Thrown when a required property is not correctly defined.
 */
public class PropertyException extends RuntimeException {
    private static final long serialVersionUID = 3109646764798499269L;

    public PropertyException() {
    }

    public PropertyException(String message) {
        super(message);
    }

    public PropertyException(Throwable cause) {
        super(cause);
    }

    public PropertyException(String message, Throwable cause) {
        super(message, cause);
    }

    public PropertyException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause);
    }
}
