package com.ms.silverking.cloud.dht.client.impl;

import com.ms.silverking.cloud.dht.client.StoredValue;
import com.ms.silverking.cloud.dht.client.serialization.BufferSourceDeserializer;
import com.ms.silverking.cloud.dht.common.RawRetrievalResult;

public class RetrievalResultMV<V> extends RetrievalResult<V> {
    private final RetrievalResultMV<V> next;
    
    public RetrievalResultMV(RawRetrievalResult rawResult, BufferSourceDeserializer<V> valueDeserializer,
                            RetrievalResultMV<V> next) {
        super(rawResult, valueDeserializer);
        this.next = next;
    }
    
    @Override
    public StoredValue<V> next() {
        return next;
    }
}
