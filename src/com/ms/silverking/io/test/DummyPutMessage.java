package com.ms.silverking.io.test;

import java.nio.ByteBuffer;

public class DummyPutMessage {
	private final ByteBuffer	buf;
	
	public DummyPutMessage(ByteBuffer buf, int offset, int length) {
		this.buf = buf.asReadOnlyBuffer();
		this.buf.position(offset);
		this.buf.limit(offset + length);
	}
	
	public ByteBuffer[] toBuffers() {
		ByteBuffer[]	bufs;
		
		bufs = new ByteBuffer[1];
		bufs[0] = buf;
		return bufs;
	}
}
