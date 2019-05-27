package com.ms.silverking.log;

import java.util.logging.Level;

class LogEntry {
	private final Object	obj;
	private final String	msg;
	private final Level		level;
	
	LogEntry (Level level, Object obj,  String msg) {
		this.level = level;
		this.obj = obj;
		this.msg = msg;
	}
	
	LogEntry (Level level, String msg) {
		this(level, null, msg);
	}
	
	void log() {
		StringBuilder	sb;
		
		sb = new StringBuilder();
		if (obj != null) {
			sb.append(obj.toString());
		}
		sb.append(' ');
		sb.append(msg);
		Log.log(level, sb.toString());
	}
}