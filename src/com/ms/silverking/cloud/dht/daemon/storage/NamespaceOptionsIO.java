package com.ms.silverking.cloud.dht.daemon.storage;

import java.io.File;
import java.io.IOException;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.io.FileUtil;

// deprecated in favor of NamespacePropertiesIO

public class NamespaceOptionsIO {
    private static final String optionsFileName = "options";
    
    private static File optionsFile(File nsDir) {
        return new File(nsDir, optionsFileName);
    }
    
    public static NamespaceOptions read(File nsDir) throws IOException {
        if (!nsDir.isDirectory()) {
            throw new IOException("NamespaceOptionsIO.read() passed non-directory: "+ nsDir);
        }
        return _read(optionsFile(nsDir));
    }
    
    private static NamespaceOptions _read(File optionsFile) throws IOException {
        return NamespaceOptions.parse(FileUtil.readFileAsString(optionsFile));
    }
    
    public static void write(File nsDir, NamespaceOptions nsOptions) throws IOException {
        if (!nsDir.isDirectory()) {
            throw new IOException("NamespaceOptionsIO.write() passed non-directory: "+ nsDir);
        }
        if (optionsFileExists(nsDir)) {
            NamespaceOptions existingOptions;
            
            existingOptions = read(nsDir);
            if (!nsOptions.equals(existingOptions)) {
                System.err.println(nsOptions);
                System.err.println(existingOptions);
                throw new RuntimeException("Existing options != nsOptions");
            }
        } else {
            _write(optionsFile(nsDir), nsOptions);
        }
    }
    
    private static void _write(File optionsFile, NamespaceOptions nsOptions) throws IOException {
        FileUtil.writeToFile(optionsFile, nsOptions.toString());
    }
    
    public static boolean optionsFileExists(File nsDir) throws IOException {
        return optionsFile(nsDir).exists();
    }
}
