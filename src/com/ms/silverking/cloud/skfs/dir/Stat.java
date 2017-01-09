package com.ms.silverking.cloud.skfs.dir;

import java.util.concurrent.ThreadLocalRandom;

import com.ms.silverking.numeric.NumConversion;

public class Stat {
	private final long	dev;
	private final long	ino;
	private final long	nlink;
	private final int	mode;
	private final int	uid;
	private final int	gid;
	private final int	gidPad;
	private final long	rdev;
	private final long	size;
	private final long	blksize;
	private final long	blocks;
	private final long	atime;
	private final long	atimePad;
	private final long	mtime;
	private final long	mtimePad;
	private final long	ctime;
	private final long	ctimePad;
	private final long	pad1;
	private final long	pad2;
	private final long	pad3;
	
	private static final int	devOffset = 0;
	private static final int	inoOffset = devOffset + NumConversion.BYTES_PER_LONG;
	private static final int	nlinkOffset = inoOffset + NumConversion.BYTES_PER_LONG;
	private static final int	modeOffset = nlinkOffset + NumConversion.BYTES_PER_LONG;
	private static final int	uidOffset = modeOffset + NumConversion.BYTES_PER_INT;
	private static final int	gidOffset = uidOffset + NumConversion.BYTES_PER_INT;
	private static final int	gidPadOffset = gidOffset + NumConversion.BYTES_PER_INT;
	private static final int	rdevOffset = gidPadOffset + NumConversion.BYTES_PER_INT;
	private static final int	sizeOffset = rdevOffset + NumConversion.BYTES_PER_LONG;
	private static final int	blksizeOffset = sizeOffset + NumConversion.BYTES_PER_LONG;
	private static final int	blocksOffset = blksizeOffset + NumConversion.BYTES_PER_LONG;
	private static final int	atimeOffset = blocksOffset + NumConversion.BYTES_PER_LONG;
	private static final int	atimePadOffset = atimeOffset + NumConversion.BYTES_PER_LONG;
	private static final int	mtimeOffset = atimePadOffset + NumConversion.BYTES_PER_LONG;
	private static final int	mtimePadOffset = mtimeOffset + NumConversion.BYTES_PER_LONG;
	private static final int	ctimeOffset = mtimePadOffset + NumConversion.BYTES_PER_LONG;
	private static final int	ctimePadOffset = ctimeOffset + NumConversion.BYTES_PER_LONG;
	private static final int	pad1Offset = ctimePadOffset + NumConversion.BYTES_PER_LONG;
	private static final int	pad2Offset = pad1Offset + NumConversion.BYTES_PER_LONG;
	private static final int	pad3Offset = pad2Offset + NumConversion.BYTES_PER_LONG;
	
	static final int	serializedLength = pad3Offset + NumConversion.BYTES_PER_LONG;
	
	private static final int	srfsBlockSize = 262144;
	
	//#define __S_IFDIR       0040000 /* Directory.  */
	//#define __S_IFCHR       0020000 /* Character device.  */
	//#define __S_IFBLK       0060000 /* Block device.  */
	//#define __S_IFREG       0100000 /* Regular file.  */
	//#define __S_IFIFO       0010000 /* FIFO.  */
	//#define __S_IFLNK       0120000 /* Symbolic link.  */
	//#define __S_IFSOCK      0140000 /* Socket.  */
	private static final int	S_IFDIR  = 0040000;	
	private static final int	S_IFCHR  = 0020000;	
	private static final int	S_IFBLK  = 0060000;	
	private static final int	S_IFREG  = 0100000;	
	private static final int	S_IFIFO  = 0010000;	
	private static final int	S_IFLNK  = 0120000;	
	private static final int	S_IFSOCK = 0140000;	
	
	public static Stat createDirStat(int mode, long size) {
		long	epochTimeSeconds;
		
		epochTimeSeconds = System.currentTimeMillis() / 1000;
		return new Stat(
					0, // dev, 
					Math.abs(ThreadLocalRandom.current().nextLong()), // ino, 
					S_IFDIR | mode, // mode, 
					2, // nlink, 
					0, // uid, 
					0, // gid, 
					0, // rdev, 
					size, // size, 
					srfsBlockSize, // blksize, 
					(size - 1) / srfsBlockSize, // blocks, 
					epochTimeSeconds, // atime, 
					epochTimeSeconds, // mtime, 
					epochTimeSeconds // ctime
					);
	}
	
	public Stat(int dev, long ino, int mode, long nlink, int uid, int gid, long rdev, 
				long size, long blksize, long blocks, 
				long atime, long mtime, long ctime) {
		this.dev = dev;
		this.ino = ino;
		this.mode = mode;
		this.nlink = nlink;
		this.uid = uid;
		this.gid = gid;
		gidPad = 0;
		this.rdev = rdev;
		this.size = size;
		this.blksize = blksize;
		this.blocks = blocks;
		this.atime = atime;
		atimePad = 0;
		this.mtime = mtime;
		mtimePad = 0;
		this.ctime = ctime;
		ctimePad = 0;
		
		pad1 = 0;
		pad2 = 0;
		pad3 = 0;
	}
	
	public int getSerializedLength () {
		return serializedLength;
	}
	
	public void serialize(byte[] dest, int offset) {
		if (offset + getSerializedLength() > dest.length) {
			throw new RuntimeException("Insufficient space for serialization");
		}
		
		NumConversion.longToBytesLittleEndian(dev, dest, offset + devOffset);
		NumConversion.longToBytesLittleEndian(ino, dest, offset + inoOffset);
		NumConversion.longToBytesLittleEndian(nlink, dest, offset + nlinkOffset);
		
		NumConversion.intToBytesLittleEndian(mode, dest, offset + modeOffset);
		NumConversion.intToBytesLittleEndian(uid, dest, offset + uidOffset);
		NumConversion.intToBytesLittleEndian(gid, dest, offset + gidOffset);
		NumConversion.intToBytesLittleEndian(gidPad, dest, offset + gidPadOffset);
		
		NumConversion.longToBytesLittleEndian(rdev, dest, offset + rdevOffset);
		NumConversion.longToBytesLittleEndian(size, dest, offset + sizeOffset);
		NumConversion.longToBytesLittleEndian(blksize, dest, offset + blksizeOffset);
		NumConversion.longToBytesLittleEndian(blocks, dest, offset + blocksOffset);
		NumConversion.longToBytesLittleEndian(atime, dest, offset + atimeOffset);
		NumConversion.longToBytesLittleEndian(atimePad, dest, offset + atimePadOffset);
		NumConversion.longToBytesLittleEndian(mtime, dest, offset + mtimeOffset);
		NumConversion.longToBytesLittleEndian(mtimePad, dest, offset + mtimePadOffset);
		NumConversion.longToBytesLittleEndian(ctime, dest, offset + ctimeOffset);
		NumConversion.longToBytesLittleEndian(ctimePad, dest, offset + ctimePadOffset);
		NumConversion.longToBytesLittleEndian(pad1, dest, offset + pad1Offset);
		NumConversion.longToBytesLittleEndian(pad2, dest, offset + pad2Offset);
		NumConversion.longToBytesLittleEndian(pad3, dest, offset + pad3Offset);
	}
	
	public static Stat deserialize(byte[] buf) {
		return deserialize(buf, 0);
	}
	
	public static Stat deserialize(byte[] buf, int offset) {
		long	dev;
		long	ino;
		long	nlink;
		int		mode;
		int		uid;
		int		gid;
		int		gidPad;
		long	rdev;
		long	size;
		long	blksize;
		long	blocks;
		long	atime;
		long	atimePad;
		long	mtime;
		long	mtimePad;
		long	ctime;
		long	ctimePad;
		long	pad1;
		long	pad2;
		long	pad3;
		
		dev = NumConversion.bytesToLongLittleEndian(buf, offset + devOffset);
		ino = NumConversion.bytesToLongLittleEndian(buf, offset + inoOffset);
		nlink = NumConversion.bytesToLongLittleEndian(buf, offset + nlinkOffset);
		
		mode = NumConversion.bytesToIntLittleEndian(buf, offset + modeOffset);
		uid = NumConversion.bytesToIntLittleEndian(buf, offset + uidOffset);
		gid = NumConversion.bytesToIntLittleEndian(buf, offset + gidOffset);
		gidPad = NumConversion.bytesToIntLittleEndian(buf, offset + gidPadOffset);
		
		rdev = NumConversion.bytesToLongLittleEndian(buf, offset + rdevOffset);
		size = NumConversion.bytesToLongLittleEndian(buf, offset + sizeOffset);
		blksize = NumConversion.bytesToLongLittleEndian(buf, offset + blksizeOffset);
		blocks = NumConversion.bytesToLongLittleEndian(buf, offset + blocksOffset);
		atime = NumConversion.bytesToLongLittleEndian(buf, offset + atimeOffset);
		atimePad = NumConversion.bytesToLongLittleEndian(buf, offset + atimePadOffset);
		mtime = NumConversion.bytesToLongLittleEndian(buf, offset + mtimeOffset);
		mtimePad = NumConversion.bytesToLongLittleEndian(buf, offset + mtimePadOffset);
		ctime = NumConversion.bytesToLongLittleEndian(buf, offset + ctimeOffset);
		ctimePad = NumConversion.bytesToLongLittleEndian(buf, offset + ctimePadOffset);
		pad1 = NumConversion.bytesToLongLittleEndian(buf, offset + pad1Offset);
		pad2 = NumConversion.bytesToLongLittleEndian(buf, offset + pad2Offset);
		pad3 = NumConversion.bytesToLongLittleEndian(buf, offset + pad3Offset);
		return new Stat((int)dev, ino, mode, nlink, uid, gid, rdev, size, blksize, blocks, atime, mtime, ctime);
	}	
	
	public long getSize() {
		return size;
	}
	
	public Stat size(long size) {
		return new Stat((int)dev, ino, mode, nlink, uid, gid, rdev, size, blksize, blocks, atime, mtime, ctime);
	}
	
	@Override
	public String toString() {
		return String.format("size=%d", size);
	}
}
