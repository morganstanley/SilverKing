package com.ms.silverking.net.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.Random;

import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

public class ChannelWriteTest {
	private final Random	random;
	
	public enum Test {buffer, array, direct, mixed};
	
	private static final int	checksumSize = 16;
	private static final int	bufferSendLimit = 16;
	
	public ChannelWriteTest() {
		random = new Random();
	}
	
	public void runTests(String[] tests, int valueSize, int numValues, int iterations) throws IOException {
		for (String test : tests) {
			runTest(Test.valueOf(test), valueSize, numValues, iterations);
		}
	}
	
	public void runTest(Test test, int valueSize, int numValues, int iterations) throws IOException {
		Stopwatch	sw;
		byte[]		value;
		byte[]		checksum;
		int			entrySize;
		int			totalBytes;
		GatheringByteChannel	outChannel;
		long		totalWritten;
		ByteBuffer	valuesBuffer;
		ByteBuffer[]	buffers;
		
		entrySize = valueSize + checksumSize;
		totalBytes = entrySize * numValues;
		value = new byte[valueSize];
		checksum = new byte[checksumSize];
		random.nextBytes(value);
		outChannel = new FileOutputStream(new File("/dev/null")).getChannel();
		
		sw = new SimpleStopwatch();
		switch (test) {
		case array:
			{
				byte[]	msg;
				
				for (int j = 0; j < iterations; j++) {
					msg = new byte[totalBytes];
					for (int i = 0; i < numValues; i++) {
						System.arraycopy(value, 0, msg, i * entrySize, valueSize);
						System.arraycopy(checksum, 0, msg, i * entrySize + valueSize, checksumSize);
					}
					valuesBuffer = ByteBuffer.wrap(msg);
					totalWritten = 0;
					while (totalWritten < totalBytes) {
						long	written;
						
						written = outChannel.write(valuesBuffer);
						if (written > 0) {
							totalWritten += written;
						}
					}
					if (totalWritten != totalBytes) {
						throw new RuntimeException("totalWritten != totalBytes");
					}
				}
			}
		break;
		case buffer:
			buffers = new ByteBuffer[numValues * 2];
			for (int i = 0; i < numValues; i++) {
				buffers[i * 2] = ByteBuffer.allocate(value.length);
				buffers[i * 2 + 1] = ByteBuffer.allocate(checksum.length);
			}
			sw.reset();
			sendBuffers(buffers, iterations, totalBytes, outChannel);
		break;
		case direct:
			buffers = new ByteBuffer[numValues * 2];
			for (int i = 0; i < numValues; i++) {
				buffers[i * 2] = ByteBuffer.allocateDirect(valueSize);
				//buffers[i * 2].put(value);
				//buffers[i * 2].flip();
				buffers[i * 2 + 1] = ByteBuffer.allocateDirect(checksumSize);
				//buffers[i * 2 + 1].put(checksum);
				//buffers[i * 2 + 1].flip();
			}
			sw.reset();
			sendBuffers(buffers, iterations, totalBytes, outChannel);
		break;
		case mixed:
			buffers = new ByteBuffer[numValues * 2];
			for (int i = 0; i < numValues; i++) {
				buffers[i * 2] = ByteBuffer.allocateDirect(valueSize);
				//buffers[i * 2].put(value);
				//buffers[i * 2].flip();
				buffers[i * 2 + 1] = ByteBuffer.allocate(checksum.length);
				//buffers[i * 2 + 1].put(checksum);
				//buffers[i * 2 + 1].flip();
			}
			sw.reset();
			sendBuffers(buffers, iterations, totalBytes, outChannel);
		break;
		}
		sw.stop();
		System.out.printf("%s\tTime per iteration %e\n", test.toString(), sw.getElapsedSeconds()/(double)iterations);
	}
	
	private void fillBuffers(ByteBuffer[] buffers) throws IOException{
		for (ByteBuffer buffer : buffers) {
			while (buffer.hasRemaining()) {
				buffer.put((byte)1);
			}
		}
	}
		
	private void sendBuffers(ByteBuffer[] buffers, int iterations, long totalToWrite, 
							GatheringByteChannel outChannel) throws IOException{
		if (false && buffers.length > bufferSendLimit) {
			int	curGroupMax;
			int	prevGroupMax;
			
			curGroupMax = Integer.MIN_VALUE;
			prevGroupMax = -1;
			while (curGroupMax < buffers.length - 1) {
				ByteBuffer[]	splitBuffers;
				long			subTotal;

				curGroupMax = Math.min(buffers.length - 1, prevGroupMax + bufferSendLimit);
				splitBuffers = new ByteBuffer[curGroupMax - prevGroupMax];
				subTotal = 0;
				for (int i = 0; i < splitBuffers.length; i++) {
					splitBuffers[i] = buffers[prevGroupMax + i + 1];
					subTotal += splitBuffers[i].capacity();
				}
				sendBuffers(splitBuffers, iterations, subTotal, outChannel);
				prevGroupMax = curGroupMax;
			}
		} else {
			long	totalWritten;
			
			fillBuffers(buffers);
			for (int j = 0; j < iterations; j++) {
				for (int i = 0; i < buffers.length; i++) {
					buffers[i].rewind();
				}
				totalWritten = 0;
				while (totalWritten < totalToWrite) {
					long	written;
					
					written = outChannel.write(buffers);
					if (written > 0) {
						totalWritten += written;
					}
				}
				if (totalWritten != totalToWrite) {
					throw new RuntimeException("totalWritten != totalToWrite");
				}
			}
		}
	}
		
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			if (args.length != 4) {
				System.out.println("<tests> <valueSize> <numValues> <iterations>");
			} else {
				String[]	tests;
				int			valueSize;
				int			numValues;
				int			iterations;
				
				tests = args[0].split(",");
				valueSize = Integer.parseInt(args[1]);
				numValues = Integer.parseInt(args[2]);
				iterations = Integer.parseInt(args[3]);
				new ChannelWriteTest().runTests(tests, valueSize, numValues, iterations);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
