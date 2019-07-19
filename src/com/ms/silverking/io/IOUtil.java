package com.ms.silverking.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;

public class IOUtil {
    public static <T> void writeAsLines(File outFile, Collection<T> c) throws IOException {
        writeAsLines(new FileOutputStream(outFile), c, true);
    }
    
    public static <T> void writeAsLines(OutputStream out, Collection<T> c, boolean close) throws IOException {
        try {
            for (T element : c) {
                out.write(element.toString().getBytes());
                out.write('\n');
            }
        } finally {
            if (close) {
                out.close();
            }
        }
    }
}
