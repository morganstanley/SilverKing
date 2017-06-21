package com.ms.silverking.cloud.dht.daemon.storage;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.client.impl.KeyCreator;
import com.ms.silverking.cloud.dht.client.impl.SimpleNamespaceCreator;
import com.ms.silverking.cloud.dht.client.serialization.internal.StringMD5KeyCreator;
import com.ms.silverking.cloud.dht.common.CCSSUtil;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.InternalRetrievalOptions;
import com.ms.silverking.cloud.dht.common.MetaDataUtil;
import com.ms.silverking.cloud.dht.common.Namespace;
import com.ms.silverking.cloud.dht.common.NamespaceProperties;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.common.SimpleValueCreator;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.dht.daemon.ActiveProxyRetrieval;
import com.ms.silverking.cloud.dht.daemon.NodeRingMaster2;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.id.UUIDBase;

abstract class DynamicNamespaceStore extends NamespaceStore {
    private final String                  name;
    protected final KeyCreator<String>    keyCreator;
    protected final byte[]                dynamicCreator;
    
    protected static final byte[] dynamicUserData = new byte[0];
    protected static final NamespaceProperties   
        dynamicNamespaceProperties = new NamespaceProperties(DHTConstants.dynamicNamespaceOptions);
    
    DynamicNamespaceStore(String name,
            MessageGroupBase mgBase, 
            NodeRingMaster2 ringMaster, 
            ConcurrentMap<UUIDBase, ActiveProxyRetrieval> activeRetrievals) {
        super(getNamespace(name).contextAsLong(), 
              null, DirCreationMode.DoNotCreateNSDir, 
              dynamicNamespaceProperties, 
              mgBase, ringMaster, false, activeRetrievals);
        this.name = name;
        keyCreator = new StringMD5KeyCreator();
        dynamicCreator = SimpleValueCreator.forLocalProcess().getBytes(); 
    }

    static Namespace getNamespace(String name) {
        return new SimpleNamespaceCreator().createNamespace(name);
    }
    
    String getName() {
        return name;
    }
    
    @Override
    protected boolean isDynamic() {
        return true;
    }

    protected void storeStaticKVPair(MessageGroupBase mgBase, long curTimeMillis, 
            DHTKey key, String value) {
        ByteBuffer  _value;
        
        _value = ByteBuffer.wrap(value.getBytes());
        storeStaticKVPair(mgBase, curTimeMillis, key, _value);
    }
    
    private void storeStaticKVPair(MessageGroupBase mgBase, long curTimeMillis, 
                                   DHTKey key, ByteBuffer value) {
        StorageParameters   storageParams;
        
        storageParams = new StorageParameters(
                                0, 
                                value.limit(),
                                StorageParameters.compressedSizeNotSet,
                                CCSSUtil.createCCSS(Compression.NONE, ChecksumType.NONE), 
                                new byte[0], 
                                dynamicCreator,
                                systemTimeSource.absTimeNanos());
        //System.out.println("storeSystemKVPair");
        _put(key, value, storageParams, dynamicUserData, DHTConstants.dynamicNamespaceOptions.getVersionMode());
    }
    
    protected ByteBuffer _retrieve(DHTKey key, InternalRetrievalOptions options) {
        ByteBuffer  value;
        
        //System.out.println("SystemNamespaceStore._retrieve()");
        value = super._retrieve(key, options);
        if (value == null) {
            byte[]  _value;
            
            _value = createDynamicValue(key, options);
            if (_value != null) {
                return createDynamicValue(key, options, _value);
            }
        }
        return value;
    }
    
    // Dynamic namespaces don't do anything special for a grouped retrieval.
    // Simply forward to the single key retrieval.
    protected ByteBuffer[] _retrieve(DHTKey[] keys, InternalRetrievalOptions options) {
        ByteBuffer[] results;

        results = new ByteBuffer[keys.length];
        for (int i = 0; i < results.length; i ++) {
        	results[i] = _retrieve(keys[i], options);
        }
        return results;
    }    

    // For now, no dynamic namespace supports writing. Return errors
    public void put(List<StorageValueAndParameters> values, byte[] userData, KeyedOpResultListener resultListener) {
        for (StorageValueAndParameters value : values) {
            resultListener.sendResult(value.getKey(), OpResult.MUTATION);
        }
    }
    
    protected abstract byte[] createDynamicValue(DHTKey key, InternalRetrievalOptions options);
    
    private ByteBuffer createDynamicValue(DHTKey key, InternalRetrievalOptions options, byte[] value) {
        ByteBuffer  buf;
        int         writeSize;
        int         compressedLength;
        int         checksumLength;
        int         storedLength;
        StorageParameters   storageParams;
        int         writeOffset;
        
        storageParams = new StorageParameters(
                                0, 
                                value.length,
                                StorageParameters.compressedSizeNotSet,
                                CCSSUtil.createCCSS(Compression.NONE, ChecksumType.NONE), 
                                new byte[0], // currently no checksum on system namespace 
                                dynamicCreator,
                                SystemTimeUtil.systemTimeSource.absTimeNanos());

        // FUTURE - could reduce duplication with WritablesSegmentBase
        compressedLength = value.length; // no compression for system values
        checksumLength = storageParams.getChecksum().length;
        storedLength = MetaDataUtil.computeStoredLength(compressedLength, checksumLength, dynamicUserData.length); 
        writeSize = storedLength + DHTKey.BYTES_PER_KEY;
        
        buf = ByteBuffer.allocate(writeSize);
        
        // FIXME - THINK ABOUT THE +1 BELOW AND THE CORRESPONDING ISSUE IN WritableSegmentBase
        writeOffset = StorageFormat.writeToBuf(key, ByteBuffer.wrap(value), storageParams, dynamicUserData, buf, new AtomicInteger(), buf.limit() + 1);
        if (writeOffset == StorageFormat.writeFailedOffset) {
            throw new RuntimeException("Unexpected failure in createDynamicValue()");
        }
        
        buf.position(DHTKey.BYTES_PER_KEY);
        
        switch (options.getRetrievalType()) {
        case VALUE:
        case VALUE_AND_META_DATA:
            buf.limit(DHTKey.BYTES_PER_KEY + storedLength);
            break;
        case META_DATA:
            buf.limit(DHTKey.BYTES_PER_KEY + MetaDataUtil.getMetaDataLength(buf, DHTKey.BYTES_PER_KEY));
            break;
        default:
            throw new RuntimeException();
        }
        buf = buf.slice();
        
        ByteBuffer _buf =  buf.allocate(buf.capacity());
        _buf.put(buf);
        _buf.rewind(); // FUTURE - look into why the copy is required
        
        return _buf;
    }
}
