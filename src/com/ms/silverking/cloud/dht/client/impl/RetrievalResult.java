package com.ms.silverking.cloud.dht.client.impl;

import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.client.MetaData;
import com.ms.silverking.cloud.dht.client.serialization.BufferSourceDeserializer;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.common.RawRetrievalResult;

/*
 * Groups OpResult of retrieval operation with the retrieved data and metadata.
 * Retrievals may result in returned data, metadata, data+metadata, or simply
 * existence results. Existence is indicated by OpResult only.
 */
class RetrievalResult<V> extends RetrievalResultBase<V> {
    private final RawRetrievalResult    rawResult;
    
	public RetrievalResult(RawRetrievalResult rawResult, BufferSourceDeserializer<V> valueDeserializer) {
	    super(valueDeserializer);
	    this.rawResult = rawResult;
	}
	
	@Override
	public OpResult getOpResult() {
	    return rawResult.getOpResult();
	}
	
    @Override
	public V getValue() {
	    if (value == valueNotSet) {
	        ByteBuffer rawValue;
	        
	        // FUTURE - have an option to perform an eager deserialization
	        rawValue = rawResult.getValue();
	        if (rawValue != null) {
	            value = valueDeserializer.deserialize(rawValue);
	        } else {
	            value = null;
	        }
	    }
		return value;
	}
	
    @Override
    public MetaData getMetaData() {
        return rawResult.getMetaData();
    }

    @Override
    public String toString() {
        StringBuilder   sb;
        
        sb = new StringBuilder();
        sb.append(value);
        sb.append(':');
        sb.append(rawResult.getMetaData());
        return sb.toString();
    }

    @Override
    public String toString(boolean labeled) {
        return MetaDataTextUtil.toMetaDataString(this, labeled);
    }
}
