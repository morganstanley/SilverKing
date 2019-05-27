package com.ms.silverking.log;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.logging.Level;

import org.apache.log4j.Logger;

class Log4jLogDest implements LogDest {
	private final Logger	logger;
	private int				level;

	private static final int	_SEVERE = Level.SEVERE.intValue();
	private static final int	_WARNING = Level.WARNING.intValue();
	private static final int	_INFO = Level.INFO.intValue();
	private static final int	_CONFIG = Level.CONFIG.intValue();
	private static final int	_FINE = Level.FINE.intValue();
	private static final int	_FINER = Level.FINER.intValue();
	private static final int	_FINEST = Level.FINEST.intValue();
	private static final int	_ALL = Level.ALL.intValue();
	
	private PrintStream	out;
	private PrintStream	err;	
	
	Log4jLogDest() {
		err = System.err;
		out = System.out;
		
		logger = Logger.getLogger("com.ms.silverking");
	}
	
	private static org.apache.log4j.Level levelToL4jLevel(Level level) {
		int intValue;
		
		intValue = level.intValue();
		if (intValue == _SEVERE) {
			return org.apache.log4j.Level.ERROR;
		} else if (intValue == _WARNING) {
			return org.apache.log4j.Level.WARN;
		} else if (intValue == _INFO) {
			return org.apache.log4j.Level.INFO;
		} else if (intValue == _CONFIG) {
			return org.apache.log4j.Level.INFO;
		} else if (intValue == _FINE) {
			return org.apache.log4j.Level.DEBUG;
		} else if (intValue == _FINER) {
			return org.apache.log4j.Level.TRACE;
		} else if (intValue == _FINEST) {
			return org.apache.log4j.Level.TRACE;
		} else if (intValue == _ALL) {
			return org.apache.log4j.Level.ALL;
		} else {
			throw new RuntimeException("Unsupported level: "+ level);
		}
	}
	
	@Override
	public void log(Level level, String msg) {
        if (levelMet(level)) {
			logger.log(levelToL4jLevel(level), msg);
        }
	}

	@Override
	public void setLevel(Level level) {
		this.level = level.intValue();
		logger.setLevel(levelToL4jLevel(level));
	}

	private boolean levelMet(Level level) {
		return this.level <= level.intValue();
	}
	
	public void setPrintStreams(OutputStream out) {
		setErrPrintStream(out);
		setOutPrintStream(out);
	}	

	private void setErrPrintStream(OutputStream out) {
		setErrPrintStream( new PrintStream(out) );
	}
	
	private void setOutPrintStream(OutputStream out) {
		setOutPrintStream( new PrintStream(out) );
	}
	
	private void setErrPrintStream(PrintStream ps) {
		err = ps;
	}
	
	private void setOutPrintStream(PrintStream ps) {
		out = ps;
	}

	@Override
	public void logError(Level l, String msg, Throwable e) {
		if (levelMet(l)) {
			logger.log(levelToL4jLevel(l), msg, e);
		}
	}	
}
