package com.ms.silverking.io.util;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.ms.silverking.io.StreamParser;
import com.ms.silverking.io.StreamParser.TrimMode;

public class ScatterColumn {
    public static void scatter(File inputFile, int scatterSize) throws IOException {
        List<String>    lines;
        
        lines = StreamParser.parseFileLines(inputFile, TrimMode.trim);
        for (int i = 0; i < lines.size(); i++) {
            System.out.print(lines.get(i) +"\t");
            if (i % scatterSize == scatterSize - 1) {
                System.out.println();
            }
        }
    }
 

    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            if (args.length != 2) {
                System.out.println("args: <file> <scatterSize>");
            } else {
                File    file;
                int     scatterSize;
                
                file = new File(args[0]);
                scatterSize = Integer.parseInt(args[1]);
                scatter(file, scatterSize);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
