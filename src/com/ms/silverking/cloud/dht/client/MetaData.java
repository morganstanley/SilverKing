package com.ms.silverking.cloud.dht.client;

import com.ms.silverking.cloud.dht.CreationTime;
import com.ms.silverking.cloud.dht.ValueCreator;

/**
 * Meta data associated with a stored value.
 */
public interface MetaData {
    /**
     * Length in bytes of the stored value. This length includes metadata.
     * @return
     */
    public int getStoredLength();
    /**
     * Length in bytes of the actual value stored ignoring compression. 
     * @return
     */
    public int getUncompressedLength();
    /**
     * Version of the value stored.
     * @return
     */
    public long getVersion();
    /**
     * Time that value was created.
     * @return
     */
    public CreationTime getCreationTime();
    /**
     * The ValueCreator responsible for storing the value.
     * @return
     */
    public ValueCreator getCreator();
    /**
     * User data associated with a value.
     * @return
     */
    public byte[] getUserData();
    /**
     * A string representation of this MetaData. 
     * @param labeled specifies whether or not to label each MetaData member
     * @return
     */
    public String toString(boolean labeled);
    /**
     * The stored checksum of this value.
     * @return
     */
    public byte[] getChecksum();
    /**
     * The Compression used to stored this value.
     * @return
     */
    public Compression getCompression();
    /**
     * The ChecksumType used to checksum this value.
     * @return
     */
    public ChecksumType getChecksumType();
}
