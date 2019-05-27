package com.ms.silverking.cloud.dht.common;

/**
 * Indicates that a value has been determined to be corrupt; e.g.
 * decompression or checksum verification may have failed.
 */
public class CorruptValueException extends Exception {
    public CorruptValueException() {
    }

    public CorruptValueException(String message) {
        super(message);
    }

    public CorruptValueException(Throwable cause) {
        super(cause);
    }

    public CorruptValueException(String message, Throwable cause) {
        super(message, cause);
    }

    public CorruptValueException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
