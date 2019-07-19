package com.ms.silverking.cloud.dht;

import com.ms.silverking.cloud.dht.client.OperationException;
import com.ms.silverking.cloud.dht.client.gen.NonVirtual;

@NonVirtual
public class SecondarySetSpecificationParseException extends OperationException {
    private final String    spec;
    
    public SecondarySetSpecificationParseException(String spec) {
        super();
        this.spec = spec;
    }

    public SecondarySetSpecificationParseException(String message,
            Throwable cause, String spec) {
        super(message, cause);
        this.spec = spec;
    }

    public SecondarySetSpecificationParseException(String message, String spec) {
        super(message);
        this.spec = spec;
    }

    public SecondarySetSpecificationParseException(Throwable cause, String spec) {
        super(cause);
        this.spec = spec;
    }

    @Override
    public String getDetailedFailureMessage() {
        return "Failed to parse secondary set specification. ";
    }    
}
