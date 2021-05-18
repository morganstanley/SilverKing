package com.ms.silverking.log;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.StreamHandler;

class JavaLogDest implements LogDest {
  private final Logger logger;
  private StreamHandler handler;
  private Formatter formatter;
  private int level;

  private PrintStream out;
  private PrintStream err;

  // Extend ConsoleHandler rather than using StreamHandler so that the OutputStream (which will often be System.out)
  // doesn't get closed during shutdown hook execution.
  private static class NonClosingStreamHandler extends ConsoleHandler {
    private NonClosingStreamHandler(final OutputStream out, final Formatter formatter) {
      setOutputStream(out);
      setFormatter(formatter);
    }
  }

  JavaLogDest() {
    /*
    err = System.err;
    out = System.out;

    logger = Logger.getLogger("com.ms.silverking");
    logger.setUseParentHandlers(false);
    formatter = new SingleLineFormatter();
    handler = createHandler(out, formatter);
    logger.addHandler(handler);
    */
    err = System.err;
    out = System.out;

    logger = Logger.getLogger("com.ms.silverking");
    logger.setUseParentHandlers(false);
    formatter = new SingleLineFormatter();
    handler = new StreamHandler(out, formatter);
    logger.addHandler(handler);
  }

  @Override
  public void log(Level level, String msg) {
    if (levelMet(level)) {
      logger.log(level, msg);
      handler.flush();
    }
  }

  private boolean levelMet(Level level) {
    return this.level <= level.intValue();
  }

  @Override
  public void setLevel(Level level) {
    this.level = level.intValue();
    logger.setLevel(level);
    handler.setLevel(level);
  }

  public void setPrintStreams(OutputStream out) {
    setErrPrintStream(out);
    setOutPrintStream(out);
    logger.removeHandler(handler);
    handler = createHandler(out, formatter);
    logger.addHandler(handler);
  }

  private void setErrPrintStream(OutputStream out) {
    setErrPrintStream(new PrintStream(out));
  }

  private void setOutPrintStream(OutputStream out) {
    setOutPrintStream(new PrintStream(out));
  }

  private void setErrPrintStream(PrintStream ps) {
    err = ps;
  }

  private void setOutPrintStream(PrintStream ps) {
    out = ps;
  }

  private StreamHandler createHandler(final OutputStream out, final Formatter formatter) {
    if (out == System.out || out == System.err)
      return new NonClosingStreamHandler(out, formatter);
    else
      return new StreamHandler(out, formatter);
  }

  @Override
  public void logError(Level l, String msg, Throwable e) {
    if (levelMet(l)) {
      logger.log(l, msg, e);
    }
  }
}
