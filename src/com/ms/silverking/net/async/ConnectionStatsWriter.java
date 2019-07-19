package com.ms.silverking.net.async;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class ConnectionStatsWriter {
    private final File   statsBaseDir;
    
    public ConnectionStatsWriter(File baseLogDir) {
        statsBaseDir = baseLogDir;
    }
    
    private File statsFile(Connection c) {
        return new File(statsBaseDir, c.getRemoteSocketAddress().toString());
    }
    
    public void writeStats(Connection c) throws IOException {
        File                _statsFile;
        RandomAccessFile    statsFile;
        
        _statsFile = statsFile(c);
        _statsFile.getParentFile().mkdirs();
        statsFile = new RandomAccessFile(_statsFile, "rw");
        statsFile.seek(statsFile.length());
        statsFile.writeBytes(System.currentTimeMillis() +"\t"+ c.statString() +"\n");
        statsFile.close();
    }
}
