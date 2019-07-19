package com.ms.silverking.cloud.dht.client;

import com.ms.silverking.cloud.dht.client.gen.NonVirtual;

@NonVirtual
public class NamespaceRecoverException extends ClientException {
    public NamespaceRecoverException() {
    }

    public NamespaceRecoverException(String arg0) {
        super(arg0);
    }

    public NamespaceRecoverException(Throwable arg0) {
        super(arg0);
    }

    public NamespaceRecoverException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }
}
