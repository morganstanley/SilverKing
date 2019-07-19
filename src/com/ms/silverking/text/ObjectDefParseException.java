package com.ms.silverking.text;

public class ObjectDefParseException extends RuntimeException {

    public ObjectDefParseException() {
    }

    public ObjectDefParseException(String message) {
        super(message);
    }

    public ObjectDefParseException(Throwable cause) {
        super(cause);
    }

    public ObjectDefParseException(String message, Throwable cause) {
        super(message, cause);
    }

    public ObjectDefParseException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
