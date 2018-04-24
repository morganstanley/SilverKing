package com.ms.silverking.cloud.dht.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.SecondaryTarget;
import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.client.crypto.EncrypterDecrypter;
import com.ms.silverking.cloud.dht.client.impl.Checksum;
import com.ms.silverking.cloud.dht.client.impl.ChecksumProvider;
import com.ms.silverking.cloud.dht.client.impl.SegmentationUtil;
import com.ms.silverking.cloud.dht.client.serialization.BufferDestSerializer;
import com.ms.silverking.cloud.dht.common.CCSSUtil;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.cloud.dht.net.protocol.KeyedMessageFormat;
import com.ms.silverking.cloud.dht.net.protocol.PutMessageFormat;
import com.ms.silverking.compression.CodecProvider;
import com.ms.silverking.compression.Compressor;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.io.util.BufferUtil;
import com.ms.silverking.numeric.NumConversion;
import com.ms.silverking.util.ArrayUtil;

/**
 * ProtoMessageGroup for put messages 
 */
public final class ProtoPutMessageGroup<V> extends ProtoValueMessageGroupBase {
    private final BufferDestSerializer<V> bdSerializer;
    private final Compressor  compressor;
    private final Checksum    checksum;
    private final boolean     checksumCompressedValues;
    private final EncrypterDecrypter	encrypterDecrypter;
    
    private static final byte[] emptyValue = new byte[0];

    public ProtoPutMessageGroup(UUIDBase uuid, long context, 
                                int putOpSize, int valueBytes, 
                                long version,
                                BufferDestSerializer<V> bdSerializer,
                                PutOptions putOptions, ChecksumType checksumType, 
                                byte[] originator,
                                byte[] creator, int deadlineRelativeMillis, 
                                EncrypterDecrypter encrypterDecrypter) {
        super(MessageType.PUT, uuid, context, putOpSize, valueBytes, 
                ByteBuffer.allocate(optionBufferLength(putOptions)), 
                PutMessageFormat.size(checksumType) - KeyedMessageFormat.baseBytesPerKeyEntry, 
                originator, deadlineRelativeMillis, ForwardingMode.FORWARD);
        Set<SecondaryTarget>   secondaryTargets;
        
        this.bdSerializer = bdSerializer;
        this.checksum = ChecksumProvider.getChecksum(checksumType);
        Compression compression = putOptions.getCompression();
        compressor = CodecProvider.getCompressor(compression);
        optionsByteBuffer.putLong(version);
        optionsByteBuffer.putShort(CCSSUtil.createCCSS(compression, checksumType));
        optionsByteBuffer.put(creator);
        secondaryTargets = putOptions.getSecondaryTargets();
        if (secondaryTargets == null) {
            optionsByteBuffer.putShort((short)0);
        } else {
            byte[]  serializedST;
            
            serializedST = SecondaryTargetSerializer.serialize(secondaryTargets);
            optionsByteBuffer.putShort((short)serializedST.length);
            optionsByteBuffer.put(serializedST);
        }
        if (putOptions.getUserData() != null) {
            optionsByteBuffer.put(putOptions.getUserData());
        }
        checksumCompressedValues = putOptions.getChecksumCompressedValues(); 
        this.encrypterDecrypter = encrypterDecrypter;
    }

    public long getVersion() {
        return optionsByteBuffer.getLong(PutMessageFormat.versionOffset);
    }
    
    private static int optionBufferLength(PutOptions putOptions) {
        return PutMessageFormat.getOptionsBufferLength(putOptions);
    }
    
    private boolean ensureMultiValueBufferValid(int valueSize) {
        if (valueBuffer == null || valueSize > valueBuffer.remaining()) {
            valueBuffer = ByteBuffer.allocate(Math.max(valueBufferSize, valueSize));
            if (!addMultiValueBuffer(valueBuffer)) {
                return false;
            }
        }        
        return true;
    }

    
    public enum ValueAdditionResult {Added, MessageGroupFull, ValueNeedsSegmentation};
    
    /**
     * Add a raw value to this message group.
     * 
     * Currently, this code eagerly serializes, compresses, and checksums this value.
     * 
     * @param dhtKey
     * @param value
     * @return
     */
    public ValueAdditionResult addValue(DHTKey dhtKey, V value) {
        boolean copyValue;
        int     uncompressedValueSize;
        int     compressedValueSize;
        int     prevPosition;
        byte[]  bytesToStore;
        int     bytesToStorePosition;
        int     bytesToStoreSize;
        byte[]  bytesToChecksum;
        int     bytesToChecksumOffset;
        int     bytesToChecksumLength;
        ByteBuffer  bytesToChecksumBuf;
        int		_bufferIndex;
        int     _bufferPosition;
        
        // FUTURE - in the future offload serialization, compression and/or checksum computation to a worker?
        // for values over some threshold? or for number of keys over some threshold?
        
        // compression
        if (compressor != null || encrypterDecrypter != null) {
            ByteBuffer  serializedBytes;
            
            serializedBytes = bdSerializer.serializeToBuffer(value);
            //System.out.println("serializedBytes: "+ serializedBytes);
            //System.out.println("serializedBytes: "+ StringUtil.byteBufferToHexString(serializedBytes));
            
            bytesToChecksum = serializedBytes.array();
            bytesToChecksumOffset = serializedBytes.position();
            bytesToChecksumLength = serializedBytes.remaining();
            uncompressedValueSize = bytesToChecksumLength;
            bytesToChecksumBuf = serializedBytes;
            
            if (serializedBytes.limit() != 0) {
                try {
                	if (compressor != null) {
                		bytesToStore = compressor.compress(serializedBytes.array(), 
                                                       serializedBytes.position(), 
                                                       serializedBytes.remaining());
	                    if (bytesToStore.length >= bytesToChecksumLength) {
	                        // If compression is not useful, then use the
	                        // uncompressed data. Note that NamespaceStore must
	                        // notice this change in order to correctly set the
	                        // compression type to NONE since the message will
	                        // still show the attempted compression type.
	                        assert serializedBytes.position() == 0;
	                        bytesToStore = serializedBytes.array();
	                    }
                	} else {
                        bytesToStore = serializedBytes.array();
                	}
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
                if (encrypterDecrypter != null) {
                	bytesToStore = encrypterDecrypter.encrypt(bytesToStore);
                	bytesToChecksumBuf = ByteBuffer.wrap(bytesToStore);
                }
            } else {
                //System.out.println("empty serialized bytes");
                bytesToStore = emptyValue;
            }
            bytesToStorePosition = 0;
            bytesToStoreSize = bytesToStore.length;
            //System.out.println("bytesToStore: "+ StringUtil.byteArrayToHexString(bytesToStore));
        } else {
            uncompressedValueSize = 0;
            bytesToStore = null;
            // FUTURE - think about whether we can avoid or reduce estimation
            bytesToStoreSize = bdSerializer.estimateSerializedSize(value);
            bytesToStorePosition = -1;
            
            bytesToChecksum = null;
            bytesToChecksumOffset = -1;
            bytesToChecksumLength = -1;
            bytesToChecksumBuf = null;
        }
        if (!canBeAdded(bytesToStoreSize)) {
            if (bytesToStoreSize <= SegmentationUtil.maxValueSegmentSize) {
                    return ValueAdditionResult.MessageGroupFull;
            } else {
                return ValueAdditionResult.ValueNeedsSegmentation;
            }
        }

        // decide whether or not to copy the value
        if (opSize == 1) {
            copyValue = false;
        } else if (bytesToStoreSize >= dedicatedBufferSizeThreshold) {
            copyValue = false;
        } else {
            copyValue = true;
        }
                
        // buffer storage
        if (copyValue) {                             
            if (!ensureMultiValueBufferValid(bytesToStoreSize)) {
                return ValueAdditionResult.MessageGroupFull;
            }
            // record where the value will be written into the key buffer
            _bufferIndex = curMultiValueBufferIndex;
            _bufferPosition = valueBuffer.position();
            prevPosition = _bufferPosition;
            if (bytesToStore == null) {
                bdSerializer.serializeToBuffer(value, valueBuffer);
                //bytesToChecksum = valueBuffer.array();
                bytesToChecksumOffset = prevPosition;
                bytesToChecksumLength = valueBuffer.position() - prevPosition;
                bytesToChecksumBuf = (ByteBuffer)valueBuffer.asReadOnlyBuffer()
                        .position(bytesToChecksumOffset).limit(prevPosition + bytesToChecksumLength);
            } else {
                assert bytesToChecksum != null;
                valueBuffer.put(bytesToStore, bytesToStorePosition, bytesToStoreSize);
            }
        } else {
            ByteBuffer  newBuf;
            
            if (bytesToStore == null) {
                newBuf = bdSerializer.serializeToBuffer(value);
                bytesToStorePosition = newBuf.position();
                bytesToStoreSize = newBuf.limit();
            } else {
                newBuf = ByteBuffer.wrap(bytesToStore, bytesToStorePosition, bytesToStoreSize);
            }
            if (bytesToChecksum == null) {
                bytesToChecksum = newBuf.array();
                bytesToChecksumOffset = newBuf.position();
                bytesToChecksumLength = newBuf.remaining();
                bytesToChecksumBuf = ByteBuffer.wrap(bytesToChecksum, bytesToChecksumOffset, bytesToChecksumLength);
            }
            //System.out.println("newBuf: "+ newBuf);
            //newBuf.position(bytesToStorePosition + bytesToStoreSize);
            //newBuf.limit(bytesToStorePosition + bytesToStoreSize);
            newBuf.position(bytesToStorePosition + bytesToStoreSize);
            
            // record where the value will be located in the key buffer
            _bufferIndex = bufferList.size();
            _bufferPosition = 0;
            if (!addDedicatedBuffer(newBuf)) {
                return ValueAdditionResult.MessageGroupFull;
            }
        }
        
        if (uncompressedValueSize == 0) {
            uncompressedValueSize = bytesToStoreSize;
        }
        compressedValueSize = bytesToStoreSize; 
        
        /*
        // key byte buffer update
        addKey(dhtKey);
        keyByteBuffer.putShort(_bufferIndex);
        keyByteBuffer.putInt(_bufferPosition);
        keyByteBuffer.putInt(uncompressedValueSize);
        keyByteBuffer.putInt(compressedValueSize);
        
        // append the checksum
        checksum.checksum(bytesToChecksumBuf, keyByteBuffer);
        */
        
        addValueHelper(dhtKey, _bufferIndex, _bufferPosition,
                uncompressedValueSize, compressedValueSize,
                bytesToChecksumBuf);
        
        //_size++;
        return ValueAdditionResult.Added;
    }

    public void addValueDedicated(DHTKey dhtKey, ByteBuffer valueBuf) {
        int _bufferIndex;
        int _bufferPosition; 
        int uncompressedValueSize;
        int compressedValueSize;

        if (!addDedicatedBuffer(valueBuf)) {
            throw new RuntimeException("Too many buffers");
        }
        _bufferIndex = bufferList.size() - 1;
        _bufferPosition = 0;
        uncompressedValueSize = valueBuf.limit();
        compressedValueSize = uncompressedValueSize;
        addValueHelper(dhtKey, _bufferIndex, _bufferPosition,
                uncompressedValueSize, compressedValueSize,
                valueBuf);
        // Handle the side effect expected by addValueHelper.
        // In the future eliminate this side effect and this code.
        if (valueBuf.position() == 0) {
            valueBuf.position(valueBuf.limit());
        }
    }
    
    // NOTE: currently this method counts on the fact that checksum will move
    // the position of the bytesToChecksumBuf. FUTURE - eliminate this side effect.
    private void addValueHelper(DHTKey dhtKey, int _bufferIndex, int _bufferPosition, 
                          int uncompressedValueSize, int compressedValueSize,
                          ByteBuffer bytesToChecksumBuf) {
        // key byte buffer update
        addKey(dhtKey);
        keyByteBuffer.putInt(_bufferIndex);
        keyByteBuffer.putInt(_bufferPosition);
        keyByteBuffer.putInt(uncompressedValueSize);
        keyByteBuffer.putInt(compressedValueSize);
        
        // append the checksum
        if (uncompressedValueSize <= compressedValueSize || checksumCompressedValues) {
            checksum.checksum(bytesToChecksumBuf, keyByteBuffer);
        } else {
            checksum.emptyChecksum(keyByteBuffer);
        }
    }

    /////////////////
    
    public static void setPutVersion(MessageGroup mg, long version) {
        mg.getBuffers()[optionBufferIndex].putLong(PutMessageFormat.versionOffset, version);
    }
    
    public static long getPutVersion(MessageGroup mg) {
        //System.out.println(mg.getBuffers()[optionBufferIndex]); 
        return mg.getBuffers()[optionBufferIndex].getLong(PutMessageFormat.versionOffset);
    }

    public static short getCCSS(MessageGroup mg) {
        //System.out.println("obb\t: "+ mg.getBuffers()[optionBufferIndex]);
        return mg.getBuffers()[optionBufferIndex].getShort(PutMessageFormat.ccssOffset);
    }

    public static ChecksumType getChecksumType(MessageGroup mg) {
        return CCSSUtil.getChecksumType(getCCSS(mg));
    }

    public static byte[] getValueCreator(MessageGroup mg) {
        byte[]  vc;
        
        vc = new byte[ValueCreator.BYTES];
        System.arraycopy(mg.getBuffers()[optionBufferIndex].array(), PutMessageFormat.valueCreatorOffset, vc, 0, ValueCreator.BYTES);
        return vc;
    }
    
    public static byte[] getUserData(MessageGroup mg, int stLength) {
        byte[]  userData;
        ByteBuffer  optionsBuffer;
        int userDataLength;
        
        optionsBuffer = mg.getBuffers()[optionBufferIndex];
        userDataLength = optionsBuffer.capacity() - PutMessageFormat.userDataOffset(stLength);
        if (userDataLength == 0) {
            userData = ArrayUtil.emptyByteArray;
        } else {
            userData = new byte[userDataLength];
            System.arraycopy(optionsBuffer.array(), PutMessageFormat.userDataOffset(stLength), userData, 0, userData.length);
        }
        return userData;
    }

    public void getMostRecentChecksum(byte[] _checksum) {
        if (_checksum.length > 0) {
            BufferUtil.get(keyByteBuffer, keyByteBuffer.limit() - _checksum.length, _checksum, _checksum.length);
        }
    }

    public static int getSTLength(MessageGroup mg) {
        return mg.getBuffers()[optionBufferIndex].getShort(PutMessageFormat.stDataOffset);
    }
    
    public static Set<SecondaryTarget> getSecondaryTargets(MessageGroup mg) {
        int     stLength;
        byte[]  stDef;
        
        stLength = getSTLength(mg);
        if (stLength == 0) {
            return null;
        } else {
            stDef = new byte[stLength];
            System.arraycopy(mg.getBuffers()[optionBufferIndex].array(), 
                    PutMessageFormat.stDataOffset + NumConversion.BYTES_PER_SHORT, stDef, 0, stLength);
            return SecondaryTargetSerializer.deserialize(stDef);
        }
    }
    
    /*
    int _size;
    
    public MessageGroup toMessageGroup() {
        System.out.println("put\t"+ _size);
        return super.toMessageGroup();
    }
    */
}
