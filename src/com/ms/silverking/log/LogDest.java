package com.ms.silverking.log;

import java.io.OutputStream;
import java.util.logging.Level;

interface LogDest {
	public void log(Level level, String msg);
	public void setLevel(Level level);
	public void setPrintStreams(OutputStream out);
	public void logError(Level l, String msg, Throwable e);
}
