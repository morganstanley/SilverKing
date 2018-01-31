package com.ms.silverking.cloud.dht.common;

import java.nio.ByteBuffer;
import java.util.logging.Level;

import com.ms.silverking.cloud.dht.CreationTime;
import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.client.MetaData;
import com.ms.silverking.cloud.dht.client.StoredValue;
import com.ms.silverking.cloud.dht.client.crypto.EncrypterDecrypter;
import com.ms.silverking.cloud.dht.client.impl.MetaDataTextUtil;
import com.ms.silverking.cloud.dht.client.impl.SegmentationUtil;
import com.ms.silverking.compression.CodecProvider;
import com.ms.silverking.compression.Decompressor;
import com.ms.silverking.log.Log;

/*
 * Groups OpResult of retrieval operation with the retrieved data and metadata.
 * Retrievals may result in returned data, metadata, data+metadata, or simply
 * existence results. Existence is indicated by OpResult only.
 * 
 * Unlike RetrievalResult, RawRetrievalResult does not have any deserialized
 * version of the result.
 */
public class RawRetrievalResult implements StoredValue<ByteBuffer> {
    private final RetrievalType   retrievalType;
	private OpResult		result;
	private ByteBuffer      storedValue; // data + metadata
    private ByteBuffer      cookedValue; // data + metadata
	
	private static final boolean   debugChecksum = false;
	
	public RawRetrievalResult(RetrievalType retrievalType) {
	    this.retrievalType = retrievalType;
		result = OpResult.INCOMPLETE;
	}
	
    @Override
    public StoredValue<ByteBuffer> next() {
        return null;
    }
	
	public OpResult getOpResult() {
		return result;
	}
	
    public void setOpResult(OpResult result) {
        setOpResult(result, false);
    }
    
	public void setOpResult(OpResult result, boolean allowReset) {
		if (!allowReset && this.result == OpResult.SUCCEEDED) {
			throw new RuntimeException("Attempted to set result for successful op");
		}
		this.result = result;
	}
	
    public void setStoredValue_direct(ByteBuffer storedValue) {
        assert storedValue != null;
        if (this.storedValue != null) {
            Log.warning("Tried to reset value");
        } else {
            this.storedValue = storedValue;
            result = OpResult.SUCCEEDED;
        }
    }
    
    public void setStoredValue(ByteBuffer storedValue, boolean verifyChecksum, boolean filterInvalidated, EncrypterDecrypter encrypterDecrypter) throws CorruptValueException {
        int    baseOffset;
        byte[] storedData;
        byte[] dataToVerify;
        int    verifyDataOffset;
        int    dataOffset;
        int    compressedLength;
        int    uncompressedLength;
        Compression compression;
        
        setStoredValue_direct(storedValue);
        dataToVerify = null;
        verifyDataOffset = 0;
        if (result == OpResult.SUCCEEDED) {
            baseOffset = storedValue.position();
            storedData = storedValue.array();
        	//System.out.printf("%d\t%d\n", baseOffset, storedData.length);
            compressedLength = MetaDataUtil.getCompressedLength(storedData, baseOffset);
            uncompressedLength = MetaDataUtil.getUncompressedLength(storedData, baseOffset);
            if (debugChecksum) {
                System.out.println("compressedLength: "+ compressedLength);
                System.out.println("uncompressedLength: "+ uncompressedLength);
            }
            dataOffset = MetaDataUtil.getDataOffset(storedData, baseOffset);
            
            if (retrievalType.hasValue()) {
            	if (encrypterDecrypter != null) {
                    byte[]	_storedData;
            		byte[]	plainText;
            		int		offset;
            		int		tailLength;
            		
            		offset = MetaDataUtil.getDataOffset(storedData, baseOffset);
            		plainText = encrypterDecrypter.decrypt(storedData, offset, compressedLength);
	                dataToVerify = new byte[compressedLength];
	                verifyDataOffset = 0;
	                System.arraycopy(storedData, offset, dataToVerify, 0, compressedLength);
            		tailLength = storedData.length - offset - compressedLength;
            		_storedData = new byte[offset + plainText.length + tailLength]; 
            		System.arraycopy(storedData, 0, _storedData, 0, offset);
            		System.arraycopy(plainText, 0, _storedData, offset, plainText.length);
            		System.arraycopy(storedData, offset + compressedLength, _storedData, offset + plainText.length, tailLength);
            		storedData = _storedData;
            	}
	            compression = EnumValues.compression[MetaDataUtil.getCompression(storedData, baseOffset)];
	            if (MetaDataUtil.isCompressed(storedData, baseOffset)) {
	                byte[]          uncompressedData;
	                Decompressor    decompressor;
	                
	                Log.fine("Compressed");
	                decompressor = CodecProvider.getDecompressor(compression);
	                if (decompressor == null) {
	                    if (compression == Compression.NONE) {
	                        throw new RuntimeException("MetaDataUtil.isCompressed() returning true for uncompressed data");
	                    } else {
	                        throw new RuntimeException("Can't find compressor for: "+ compression);
	                    }
	                }
	                try {
	                    //System.out.println(compression +" "+ decompressor);
	                    uncompressedData = decompressor.decompress(storedData, dataOffset, compressedLength, uncompressedLength);
	                    if (encrypterDecrypter == null) {
	                    	dataToVerify = uncompressedData;
	                    	verifyDataOffset = 0;
	                    }
	                } catch (Exception e) {
	                	if (Log.levelMet(Level.INFO)) {
	                		Log.logErrorWarning(e);
	                	}
	                    throw new CorruptValueException(e);
	                }
	            } else {
                    if (encrypterDecrypter == null) {
		                dataToVerify = storedData;
		                verifyDataOffset = dataOffset;
                    }
	            }
            } else {
                dataToVerify = storedData;
                verifyDataOffset = dataOffset;
            }
            
            if (filterInvalidated && ValueUtil.isInvalidated(storedData, baseOffset)) {
            	result = OpResult.NO_SUCH_VALUE;
            } else {
                if (retrievalType.hasValue()) {
		            if (verifyChecksum) {
		                int	   verifyDataLength;
		                
		                verifyDataLength = dataToVerify.length - verifyDataOffset;
		                ValueUtil.verifyChecksum(storedData, baseOffset, dataToVerify, verifyDataOffset, verifyDataLength);
		            }
		            cookedValue = checkedWrap(dataToVerify, verifyDataOffset, uncompressedLength);
		            //cookedValue = checkedWrap(storedData, dataOffset, uncompressedLength);
                }
            }
        }
    }
    
	public ByteBuffer getValue() {
	    if (!retrievalType.hasValue() || result != OpResult.SUCCEEDED) {
	        return null;
        } else {
            return cookedValue;
	    }
	}
	    
	private ByteBuffer checkedWrap(byte[] array, int offset, int length) {
        try {
            return ByteBuffer.wrap(array, offset, length);
        } catch (RuntimeException re) {
            re.printStackTrace();
            System.out.println(array.length + " " + offset + " " + length + " "
                    + ((offset + length > array.length) ? "!!!!" : ""));
            throw re;
        }
	}

    public ByteBuffer getValueForSegmentedMetaData() {
        int    baseOffset;
        byte[] storedData;
        int    dataOffset;
        int    compressedLength;
        int    uncompressedLength;
        
        baseOffset = storedValue.position();
        storedData = storedValue.array();
        compressedLength = MetaDataUtil.getCompressedLength(storedData, baseOffset);
        uncompressedLength = MetaDataUtil.getUncompressedLength(storedData, baseOffset);
        if (debugChecksum) {
            System.out.println("compressedLength: "+ compressedLength);
            System.out.println("uncompressedLength: "+ uncompressedLength);
        }
        dataOffset = MetaDataUtil.getDataOffset(storedData, baseOffset);
        try {
            return ByteBuffer.wrap(storedData, dataOffset, uncompressedLength).slice();
        } catch (RuntimeException re) {
            re.printStackTrace();
            System.out.println(storedData.length +" "+ dataOffset +" "+ uncompressedLength 
                    +" "+ ((dataOffset + uncompressedLength > storedData.length) ? "!!!!" : ""));
            throw re;
        }
    }
	
	public MetaData getMetaData() {
	    if (!isSegmented()) {
	        if (!retrievalType.hasMetaData() || result != OpResult.SUCCEEDED) {
                return null;
	        } else {
                return this;
	        }
	    } else {
	        return SegmentationUtil.getMetaData(this, getValueForSegmentedMetaData());
	    }
	}
	
	void setResult(OpResult result) {
		if (result != OpResult.INCOMPLETE) {
			throw new RuntimeException("Attempted to reset result of "+ this +" to "+ result);
		}
		this.result = result;
	}
	
	OpResult getResult() {
		return result;
	}
	
	/*
	static RawRetrievalResult[] newArray(int size) {
		RawRetrievalResult[]	retrievalResults;
		
		retrievalResults = new RawRetrievalResult[size];
		for (int i = 0; i < retrievalResults.length; i++) {
			retrievalResults[i] = new RawRetrievalResult();
		}
		return retrievalResults;
	}
	*/
	
	// begin MetaData implementation
	
    @Override
    public int getStoredLength() {
        return MetaDataUtil.getStoredLength(storedValue.array(), storedValue.position());
    }

    @Override
    public int getUncompressedLength() {
        return MetaDataUtil.getUncompressedLength(storedValue.array(), storedValue.position());
    }

    @Override
    public long getVersion() {
        return MetaDataUtil.getVersion(storedValue.array(), storedValue.position());
    }

    @Override
    public CreationTime getCreationTime() {
        return new CreationTime(getCreationTimeRaw());
    }

    public long getCreationTimeRaw() {
        return MetaDataUtil.getCreationTime(storedValue.array(), storedValue.position());
    }
    
    @Override
    public ValueCreator getCreator() {
        return MetaDataUtil.getCreator(storedValue.array(), storedValue.position());
    }
    
    public boolean isSegmented() {
    	if (getOpResult() == OpResult.SUCCEEDED) {
    		return MetaDataUtil.isSegmented(storedValue.array(), storedValue.position());
    	} else {
    		return false;
    	}
    }

    @Override
    public byte[] getUserData() {
        return MetaDataUtil.getUserData(storedValue.array(), storedValue.position());
    }
	
    @Override
    public byte[] getChecksum() {
        return MetaDataUtil.getChecksum(storedValue.array(), storedValue.position());
    }
    
    // end MetaData implementation
    
	@Override
	public String toString() {
		StringBuilder	sb;
		
		sb = new StringBuilder();
		sb.append(storedValue);
		//sb.append(':');
		//sb.append(metaData);
		return sb.toString();
	}

    @Override
    public String toString(boolean labeled) {
        return MetaDataTextUtil.toMetaDataString(this, labeled);
    }
    
    public short getCCSS() {
        return MetaDataUtil.getCCSS(storedValue.array(), storedValue.position());
    }
    
    @Override
    public Compression getCompression() {
        return EnumValues.compression[MetaDataUtil.getCompression(storedValue.array(), storedValue.position())];
    }
    
    @Override
    public ChecksumType getChecksumType() {
        return MetaDataUtil.getChecksumType(storedValue.array(), storedValue.position());
    }
}
