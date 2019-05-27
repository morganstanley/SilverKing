package com.ms.silverking.log;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.MessageFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

/**
 * 
 */
public class HighTemporalResolutionFormatter extends Formatter {
	private static AtomicLong	lastTimeNanos;
	
	Date dat = new Date();
	private final static String format = "{0,date} {0,time}";
	private MessageFormat formatter;

	private Object args[] = new Object[1];
	
	static {
		lastTimeNanos = new AtomicLong();
	}

	// Line separator string.  This is the value of the line.separator
	// property at the moment that the SimpleFormatter was created.
	private String lineSeparator = (String) java.security.AccessController
			.doPrivileged(new sun.security.action.GetPropertyAction("line.separator"));

	/**
	 * Format the given LogRecord.
	 * @param record the log record to be formatted.
	 * @return a formatted log record
	 */
	public synchronized String format(LogRecord record) {
		long	timeNanos;
		long	delta;
		
		StringBuilder sb = new StringBuilder();
		// Minimize memory allocations here.
		dat.setTime(record.getMillis());
		args[0] = dat;
		StringBuffer text = new StringBuffer();
		if (formatter == null) {
			formatter = new MessageFormat(format);
		}
		formatter.format(args, text, null);
		sb.append(text);
		sb.append(" ");
		if (record.getSourceClassName() != null) {
			sb.append(record.getSourceClassName());
		} else {
			sb.append(record.getLoggerName());
		}
		if (record.getSourceMethodName() != null) {
			sb.append(" ");
			sb.append(record.getSourceMethodName());
		}
		
		timeNanos = System.nanoTime();
		delta = timeNanos - lastTimeNanos.get();
		lastTimeNanos.set(timeNanos);
		
		sb.append(' ');
		sb.append((timeNanos / 1000000) % 1000);		
		sb.append(' ');
		sb.append((timeNanos / 1000) % 1000);		
		sb.append(' ');
		sb.append(timeNanos % 1000);		
		sb.append('\t');
		sb.append(delta);
		sb.append(lineSeparator);
		String message = formatMessage(record);
		sb.append(record.getLevel().getLocalizedName());
		sb.append(": ");
		sb.append(message);
		sb.append(lineSeparator);
		if (record.getThrown() != null) {
			try {
				StringWriter sw = new StringWriter();
				PrintWriter pw = new PrintWriter(sw);
				record.getThrown().printStackTrace(pw);
				pw.close();
				sb.append(sw.toString());
			} catch (Exception ex) {
			}
		}
		return sb.toString();
	}
}
