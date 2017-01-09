package com.ms.silverking.cloud.skfs.dir;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import com.ms.silverking.numeric.NumConversion;
import com.ms.silverking.text.StringUtil;

public class FileID {
	private final FileSystem	fileSystem;
	private final int			hash;
	private final long			instance;
	private final long			sequence;
	
	private static final int	fileSystemOffset = 0;
	private static final int	hashOffset = fileSystemOffset + NumConversion.BYTES_PER_INT; // C is using a full int for its enum
	private static final int	instanceOffset = hashOffset + NumConversion.BYTES_PER_INT;
	private static final int	sequenceOffset = instanceOffset + NumConversion.BYTES_PER_LONG;
	private static final int	padding = 16; // size of unused native attr union members
	
	static final int	serializedLength = sequenceOffset + NumConversion.BYTES_PER_LONG + padding;
	
	private static final long		localInstance;
	private static final AtomicLong	nextSequence;
	
	static {
		localInstance = ThreadLocalRandom.current().nextLong();
		nextSequence = new AtomicLong();
	}
	
	public FileID(FileSystem fileSystem, int hash, long instance, long sequence) {
		this.fileSystem = fileSystem;
		this.hash = hash;
		this.instance = instance;
		this.sequence = sequence;
	}
	
	public FileID(FileSystem fileSystem, long instance, long sequence) {
		FileID	hashFID;
		byte[]	b;
		
		hashFID = new FileID(fileSystem, 0, instance, sequence);
		b = hashFID.serialize();
		
		this.fileSystem = fileSystem;
		this.hash = mem_hash(b);
		this.instance = instance;
		this.sequence = sequence;
	}
	
	public static FileID generateSKFSFileID() {
		return new FileID(FileSystem.fsSKFS, localInstance, nextSequence.getAndIncrement());
	}
	
	public int getSerializedLength() {
		return serializedLength;
	}
	
	public void serialize(byte[] dest, int offset) {
		dest[offset + fileSystemOffset] = (byte)fileSystem.ordinal();
		NumConversion.intToBytesLittleEndian(hash, dest, offset + hashOffset);
		NumConversion.longToBytesLittleEndian(instance, dest, offset + instanceOffset);
		NumConversion.longToBytesLittleEndian(sequence, dest, offset + sequenceOffset);
	}
	
	public static FileID deserialize(byte[] buf) {
		return deserialize(buf, 0);
	}
	
	public static FileID deserialize(byte[] buf, int offset) {
		FileSystem	fileSystem;
		int			hash;
		long		instance;
		long		sequence;
		
		fileSystem = FileSystem.values()[buf[offset + fileSystemOffset]];
		hash = NumConversion.bytesToIntLittleEndian(buf, offset + hashOffset);
		instance = NumConversion.bytesToLongLittleEndian(buf, offset + instanceOffset);
		sequence = NumConversion.bytesToLongLittleEndian(buf, offset + sequenceOffset);
		return new FileID(fileSystem, hash, instance, sequence);
	}
	
	public byte[] serialize() {
		byte[]	buf;
		
		buf = new byte[getSerializedLength()];
		serialize(buf, 0);
		return buf;
	}
	
	private static int mem_hash(byte[] m) {
		return mem_hash(m, 0, m.length);
	}
	
	// Java port of Util.c mem_hash()
	private static int mem_hash(byte[] m, int offset, int size) {
	    long hash;
		int	i;
	    int c;
	    
	    if (offset + size > m.length) {
	    	throw new RuntimeException("offset + size >= m.length");
	    }
	   
	    hash = 0;
		for (i = 0; i < size; i++) {
		    c = m[offset + i];
			hash = c + (hash << 6) + (hash << 16) - hash;
	    }
	    return (int)(hash & 0xffffffff);
	}
	
	@Override
	public String toString() {
		return String.format("%d.%d.%d.%d", fileSystem.ordinal(), hash, instance, sequence);
	}

	public static void main(String[] args) {
		/*
		byte[]	m;
		
		m = args[0].getBytes();
		System.out.printf("%s\t%x\n", args[0], mem_hash(m, 0, m.length));
		*/
		for (int i = 0; i < 5; i++) {
			System.out.printf("%s\n", StringUtil.byteArrayToHexString(generateSKFSFileID().serialize()));
		}
	}
}
