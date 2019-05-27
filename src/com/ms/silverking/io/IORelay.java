// IORelay.java


package com.ms.silverking.io;


import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.ms.silverking.log.Log;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.text.StringUtil;


/**
 * <p>
 * A java "pipe" implementation. Simply connects an InputStream and an
 * OutputStream.
 * </p>
 */
public class IORelay implements Runnable {
	protected BufferedInputStream	in;
	protected BufferedOutputStream	out;
	protected boolean				running;
	protected boolean				drain;
	protected boolean				close;
	protected boolean				displayData;
	
	public static final int	DEF_BUFFER_SIZE = 1460;

	public IORelay(InputStream in, OutputStream out, int bufferSize, boolean close, boolean displayData) {
		this.in = new BufferedInputStream(in, bufferSize);
		this.out = new BufferedOutputStream(out, bufferSize);
		this.close = close;
		this.displayData = displayData;
	}
	
	public IORelay(InputStream in, OutputStream out, int bufferSize, boolean close) {
		this(in, out, bufferSize, close, false);
	}
	
	public IORelay(InputStream in, OutputStream out, int bufferSize) {
		this(in, out, bufferSize, true);
	}
	
	public IORelay(InputStream in, OutputStream out) {
		this(in, out, DEF_BUFFER_SIZE);
	}	

	public IORelay(InputStream in, OutputStream out, boolean close) {
		this(in, out, DEF_BUFFER_SIZE, close);
	}		
	
	public static IORelay fileRelay(InputStream in, String fileName) throws IOException {
		return new IORelay( in, new FileOutputStream(fileName) );
	}

	public static IORelay fileRelay(InputStream in, File file) throws IOException {
		return new IORelay( in, new FileOutputStream(file) );
	}
	
	public static void streamToFile(InputStream in, String fileName) throws IOException {
		fileRelay(in, fileName).startAndWait();
	}

	public static void streamToFileInBackground(InputStream in, File file) throws IOException {
		fileRelay(in, file).start();
	}
	
	public static void streamToFile(InputStream in, File file) throws IOException {
		fileRelay(in, file).startAndWait();
	}

	public static void streamFromFile(String fileName, OutputStream out) throws IOException {
		streamFromFile(new File(fileName), out);
	}
	
	public static void streamFromFile(File file, OutputStream out) throws IOException {
		stream(new FileInputStream(file), out);
	}
	
	public static void stream(InputStream in, OutputStream out) throws IOException {
		new IORelay(in, out).startAndWait();
	}

	public static void appendFromFile(File file, OutputStream out) throws IOException {
		append(new FileInputStream(file), out);
	}
	
	public static void append(InputStream in, OutputStream out) throws IOException {
		new IORelay(in, out, false).startAndWait();
	}
	
	public void start() {
		running = true;
		ThreadUtil.newDaemonThread(this, "IORelay").start();
	}
	
	public void stop() {
		running = false;
	}
	
	public void stopAndDrain() {
		drain = true;
		waitFor();
	}	
	
	public void waitFor() {
		synchronized (this) {
			while (running) {
				try {
					this.wait();
				} catch (InterruptedException ie) {
				}
			}
		}
	}
	
	public void startAndWait() {
		start();
		waitFor();
	}
	
	public void run() {
		try {
			byte[]	buf = new byte[DEF_BUFFER_SIZE];
			while (running) {
				int	numRead;
				
				numRead = 0;
				do {
					if (displayData) {
						System.out.printf("r...");
					}
					numRead = in.read(buf);
					if (displayData) {
						System.out.printf("%d\n", numRead);
					}
					if (numRead > 0) {
						if (displayData) {
							System.out.printf("w %d %s...", numRead, StringUtil.byteArrayToHexString(buf, 0, numRead), new String(buf, 0, numRead));
						}
						out.write(buf, 0, numRead);
						out.flush();
						if (displayData) {
							System.out.printf(";\n");
						}
					}
				} while (numRead >= 0);
				running = false;
			}
		} catch (IOException ioe) {
			if (!drain) {
				// Bad file descriptor error (ok because process is dying)
				Log.logErrorWarning(ioe);
			}
		} finally {
			if (close && in != null) {
				try {
					in.close();
				} catch (IOException ioe_close) {
					Log.fine("Exception closing IORelay input: "+ ioe_close);
				}
			}
			if (close && out != null) {
				try {
					out.close();
				} catch (IOException ioe_close) {
					Log.fine("Exception closing IORelay output: "+ ioe_close);
				}
			}
			synchronized (this) {
				running = false;
				this.notifyAll();
			}
		}
	}	
}
