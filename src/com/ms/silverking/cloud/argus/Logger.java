package com.ms.silverking.cloud.argus;

/*
import java.io.File;
import java.io.FileOutputStream;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.ms.silverking.log.Log;
import com.ms.silverking.thread.ThreadUtil;
*/

public class Logger{// implements Runnable {
    /*
    private final File                      logDir;
    private final BlockingQueue<LogEntry>   messageQueue;
    
    public Logger(File logDir) {
        messageQueue = new LinkedBlockingQueue<>();
        ThreadUtil.newDaemonThread(this, "Argus Logger");
        out = new FileOutputStream(logFile);
    }
    
    public void log(String message) {
        try {
            messageQueue.put(new LogEntry(message));
        } catch (InterruptedException ie) {
        }
    }

    @Override
    public void run() {
        while (true) {
            try {
                LogEntry    entry;
                
                entry = messageQueue.take();
                out.write((new Date(entry.getTime()) +"\t"+ entry.getMessage()).getBytes());
            } catch (Exception e) {
                Log.logErrorWarning(e);
            }
        }
    }
    
    private static class LogEntry {
        private final long      time;
        private final String    message;
        
        LogEntry(String message) {
            time = System.currentTimeMillis();
            this.message = message;
        }
        
        long getTime() {
            return time;
        }
        
        String getMessage() {
            return message;
        }
    }
    */
}
