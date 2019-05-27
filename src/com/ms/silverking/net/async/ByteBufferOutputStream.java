package com.ms.silverking.net.async;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;


/**
 */
public class ByteBufferOutputStream extends ByteArrayOutputStream {
	private List<ByteBuffer>	byteBuffers;
	
	public ByteBufferOutputStream() {
		byteBuffers = new LinkedList<ByteBuffer>();
	}
	
	@Override
	public synchronized void write(byte[] b, int off, int len) {
		super.write(b, off, len);
	}
	
	public ByteBuffer[] toBuffers() {
		/*
		byte[]		objBytes;
		ByteBuffer	objByteBuffer;
		
		objBytes = toByteArray();
		*/
		return null;
	}
}
