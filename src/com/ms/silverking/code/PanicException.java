package com.ms.silverking.code;

/**
 * Thrown to indicate a panic condition.
 */
public class PanicException extends RuntimeException {
    public PanicException() {
        super();
    }

    public PanicException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public PanicException(String message, Throwable cause) {
        super(message, cause);
    }

    public PanicException(String message) {
        super(message);
    }

    public PanicException(Throwable cause) {
        super(cause);
    }
}
