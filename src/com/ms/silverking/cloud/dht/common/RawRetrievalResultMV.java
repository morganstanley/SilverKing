package com.ms.silverking.cloud.dht.common;

import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.client.StoredValue;

/**
 * Multi-versioned RawRetrievalResult
 */
public class RawRetrievalResultMV extends RawRetrievalResult {
    private final RawRetrievalResultMV  next;
    
    public RawRetrievalResultMV(RetrievalType retrievalType, RawRetrievalResultMV next) {
        super(retrievalType);
        this.next = next;
    }
    
    @Override
    public StoredValue<ByteBuffer> next() {
        return next;
    }
}
