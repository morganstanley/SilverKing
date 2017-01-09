// FileUtil.java


package com.ms.silverking.io;


import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


public class FileUtil {
	public static void writeToFile(File file, byte[] value) throws IOException {
		FileOutputStream	out;
		
		out = new FileOutputStream(file);
		out.write(value);
		out.close();
	}		
	
	public static void writeToFile(String fileName, String text) throws IOException {
		writeToFile(new File(fileName), text);
	}
	
	public static void writeToFile(File file, String text) throws IOException {
		BufferedWriter	out;
		
		out = new BufferedWriter( 
			new OutputStreamWriter(new FileOutputStream(file)) );
		out.write(text);
		out.close();
	}	

	public static void writeToFile(String fileName, Collection lines) throws IOException {
		writeToFile(new File(fileName), lines);
	}
	
	public static void writeToFile(File file, Collection lines) throws IOException {
		BufferedWriter	out;
		
		out = new BufferedWriter( 
			new OutputStreamWriter(new FileOutputStream(file)) );
		for (Object line : lines) {
			out.write(line.toString() +"\n");
		}
		out.close();
	}		
	
	public static File[] namesToFiles(String[] names) {
		File[]	files;
		
		files = new File[names.length];
		for (int i = 0; i < files.length; i++) {
			files[i] = new File(names[i]);
		}
		return files;
	}
	
	public static byte[] readFileAsBytes(File file) throws IOException {
		byte[]	buf;
		FileInputStream	fIn;
		
		buf = new byte[(int)file.length()]; 
		
		fIn = new FileInputStream(file);
		StreamUtil.readBytes( buf, 0, buf.length, 
				new BufferedInputStream(fIn) );
		fIn.close();
		return buf;
	}
	
	public static String readFileAsString(File file) throws IOException {
		return new String(readFileAsBytes(file));
	}

    public static void cleanDirectory(File dir) {
        for(File file : dir.listFiles()) {
            file.delete();
        }
    }
    
    public static List<File> listFilesRecursively(File path) {
    	List<File>	list;
    	
    	list = new ArrayList<>();
    	listFilesRecursively(path, list);
    	return list;
    }
    
    private static void listFilesRecursively(File path, List<File> list) {
    	File[]	files;
    	
    	files = path.listFiles();
    	for (File file : files) {
    		list.add(file);
    		if (file.isDirectory()) {
    			listFilesRecursively(file, list);
    		}
    	}
    }
}
