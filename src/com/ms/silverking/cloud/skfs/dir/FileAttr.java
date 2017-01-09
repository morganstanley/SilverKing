package com.ms.silverking.cloud.skfs.dir;

import com.ms.silverking.text.StringUtil;

public class FileAttr {
	private final FileID	fid;
	private final Stat		stat;
	
	private static final int 	serializedLength = FileID.serializedLength + Stat.serializedLength;

	public FileAttr(FileID fid, Stat stat) {
		this.fid = fid;
		this.stat = stat;
	}
	
	public int getSerializedLength() {
		return serializedLength;
	}
	
	public void serialize(byte[] dest, int offset) {
		if (offset + getSerializedLength() > dest.length) {
			throw new RuntimeException("Insufficient space for serialization");
		}
		
		fid.serialize(dest, offset);
		stat.serialize(dest, offset + fid.getSerializedLength());
	}
	
	public byte[] serialize() {
		byte[]	b;
		
		b = new byte[getSerializedLength()];
		serialize(b, 0);
		return b;
	}
	
	public static FileAttr deserialize(byte[] buf) {
		if (buf.length != serializedLength) {
			throw new RuntimeException("buf.length != serializedLength");
		} else {
			FileID	fid;
			Stat	stat;
			
			fid = FileID.deserialize(buf, 0);
			stat = Stat.deserialize(buf, FileID.serializedLength);
			return new FileAttr(fid, stat);
		}
	}
	
	public FileID getFileID() {
		return fid;
	}
	
	public Stat getStat() {
		return stat;
	}
	
	@Override
	public String toString() {
		return fid.toString() + ":"+ stat.toString();
	}
	
	public static void main(String[] args) {
		/*
		byte[]	m;
		
		m = args[0].getBytes();
		System.out.printf("%s\t%x\n", args[0], mem_hash(m, 0, m.length));
		*/
		for (int i = 0; i < 5; i++) {
			FileAttr	fa;
			Stat		s;

			s = new Stat(0x0102, //dev, 
					0x0304, //ino, 
					0x0506, //mode, 
					0, //nlink, 
					0, //uid, 
					0, //gid, 
					0, //rdev, 
					0, //size, 
					0x090a, //blksize, 
					0x0708, //blocks, 
					0, //atime, 
					0, //mtime, 
					0 //ctime
					);
			/*
			s = new Stat(0, //dev, 
							0, //ino, 
							0, //mode, 
							0, //nlink, 
							0, //uid, 
							0, //gid, 
							0, //rdev, 
							0, //size, 
							0, //blksize, 
							0, //blocks, 
							0, //atime, 
							0, //mtime, 
							0 //ctime
							);
							*/
			fa = new FileAttr(FileID.generateSKFSFileID(), s);
			System.out.printf("%s\n", StringUtil.byteArrayToHexString(fa.serialize()));
		}
	}	
}
