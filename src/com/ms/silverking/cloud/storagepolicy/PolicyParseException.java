package com.ms.silverking.cloud.storagepolicy;

import java.io.IOException;

public class PolicyParseException extends IOException {
    public PolicyParseException() {
        super();
    }

    public PolicyParseException(String message, Throwable cause) {
        super(message, cause);
    }

    public PolicyParseException(String message) {
        super(message);
    }

    public PolicyParseException(Throwable cause) {
        super(cause);
    }
}
