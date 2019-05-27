package com.ms.silverking.log;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.MessageFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

/**
 * Single line log format
 */
public class SingleLineFormatter extends Formatter {
	Date dat = new Date();
    private final static String format = "{0,date,yyyy:MM:dd} {0,time,HH:mm:ss z}";
	private MessageFormat msgFormat;

	private Object args[] = new Object[1];
	
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
		StringBuilder sb = new StringBuilder();
		
        sb.append(record.getLevel().getLocalizedName());
        sb.append(": ");
		
		// Minimize memory allocations here.
		dat.setTime(record.getMillis());
		args[0] = dat;
		StringBuffer text = new StringBuffer();
		if (msgFormat == null) {
			msgFormat = new MessageFormat(format);
		}
		msgFormat.format(args, text, null);
		sb.append(text);
		sb.append(" ");
		/*
		if (record.getSourceClassName() != null) {
			sb.append(record.getSourceClassName());
		} else {
			sb.append(record.getLoggerName());
		}
		if (record.getSourceMethodName() != null) {
			sb.append(" ");
			sb.append(record.getSourceMethodName());
		}
		*/
		
		String message = formatMessage(record);
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
