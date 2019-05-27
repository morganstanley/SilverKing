package com.ms.silverking.io.test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

public class PureBufferedSerializationTest {
	public enum Test {serialization};
	public enum AllocationMethod {byteBuffer, directBuffer};
	
	public PureBufferedSerializationTest() {
	}
	
	public void runTest(Test test, AllocationMethod allocationMethod, int size, int iterations) {
		Stopwatch		sw;
		DummyPutMessage	dummyMessage;
		long			length;
		double			secondsPerIteration;
		ByteBuffer		buf;
		
		length = 0;
		switch (allocationMethod) {
		case byteBuffer:
			buf = ByteBuffer.allocate(size); 
			break;
		case directBuffer:
			buf = ByteBuffer.allocateDirect(size); 
			break;
		default: throw new RuntimeException("panic");
		}
		while (buf.hasRemaining()) {
			buf.put((byte)7);
		}
		dummyMessage = new DummyPutMessage(buf, 0, size);
		sw = new SimpleStopwatch();
		switch (test) {
		case serialization:
			sw.reset();
			for (int i = 0; i < iterations; i++) {
				ByteBuffer[]	buffers;
				
				buffers = dummyMessage.toBuffers();
				length += buffers[0].limit();
			}
			break;
		default: throw new RuntimeException("");
		}
		sw.stop();
		System.out.println(length +" "+ sw);
		secondsPerIteration = sw.getElapsedSecondsBD().divide(new BigDecimal(iterations)).doubleValue();
		System.out.printf("Time per iteration: %.2e\n", secondsPerIteration);
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			if (args.length < 4) {
				System.out.println("args: <test> <allocationMethod> <size> <iterations>");
			} else {
				Test				test;
				AllocationMethod	allocationMethod;
				int					size;
				int					iterations;
				
				test = Test.valueOf(args[0]);
				allocationMethod = AllocationMethod.valueOf(args[1]);
				size = Integer.parseInt(args[2]);
				iterations = Integer.parseInt(args[3]);
				new PureBufferedSerializationTest().runTest(test, allocationMethod, size, iterations);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
