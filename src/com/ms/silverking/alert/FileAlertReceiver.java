package com.ms.silverking.alert;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import com.ms.silverking.log.Log;

/**
 * Simple AlertReceiver implementation that stores alerts to a temporary file. Intended mainly for testing.
 */
public class FileAlertReceiver implements AlertReceiver {
  private final Writer writer;

  public FileAlertReceiver() {
    try {
      writer = new BufferedWriter(
          new OutputStreamWriter(new FileOutputStream(File.createTempFile("FileAlertReceiver", ".txt"))));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void sendAlert(Alert alert) {
    try {
      writer.write(alert.toString() + "\n");
      writer.flush();
    } catch (IOException ioe) {
      Log.logErrorWarning(ioe);
    }
  }
}
