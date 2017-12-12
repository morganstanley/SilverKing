package com.ms.silverking.cloud.dht.client;

import com.ms.silverking.cloud.dht.client.gen.NonVirtual;

@NonVirtual
public class NamespaceDeletionException extends ClientException {
    public NamespaceDeletionException() {
        super();
    }

    public NamespaceDeletionException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

    public NamespaceDeletionException(String arg0) {
        super(arg0);
    }

    public NamespaceDeletionException(Throwable arg0) {
        super(arg0);
    }
}
