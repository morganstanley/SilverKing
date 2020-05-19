package com.ms.silverking.log;

import java.io.OutputStream;
import java.util.logging.Level;

public interface LogDest {
  void log(Level level, String msg);

  void setLevel(Level level);

  void setPrintStreams(OutputStream out);

  void logError(Level l, String msg, Throwable e);
}
