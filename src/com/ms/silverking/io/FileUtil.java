// FileUtil.java


package com.ms.silverking.io;


import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.io.Files;
import com.ms.silverking.log.Log;


public class FileUtil {
	public static void writeToFile(File file, byte[] value) throws IOException {
		FileOutputStream	out;
		
		out = new FileOutputStream(file);
		try {
			out.write(value);
		} finally {
			out.close();
		}
	}		
	
	public static void writeToFile(File file, byte[] ... values) throws IOException {
		FileOutputStream	out;
		
		out = new FileOutputStream(file);
		try {
			for (byte[] value : values) {
				out.write(value);
			}
		} finally {
			out.close();
		}
	}
	
	public static void writeToFile(File file, ByteBuffer value) throws IOException {
		FileOutputStream	out;
		
		out = new FileOutputStream(file);
		try {
			out.getChannel().write(value);
		} finally {
			out.close();
		}
	}
	
	public static void writeToFile(File file, ByteBuffer ... values) throws IOException {
		FileOutputStream	out;
		
		out = new FileOutputStream(file);
		try {
			for (ByteBuffer value : values) {
				out.getChannel().write(value);
			}
		} finally {
			out.close();
		}
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
	
	public static String readFileAsString(String file) throws IOException {
		return readFileAsString(new File(file));
	}
	
	public static String readFileAsString(File file) throws IOException {
		return new String(readFileAsBytes(file));
	}

    public static void cleanDirectory(File dir) {
        for (File file : dir.listFiles()) {
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
    	if (files != null) {
	    	for (File file : files) {
	    		list.add(file);
	    		if (file.isDirectory()) {
	    			listFilesRecursively(file, list);
	    		}
	    	}
    	}
    }
	
	public static void copyDirectories(File src, File dest) {
		if (src.isFile()) {
			throw new RuntimeException("src needs to a be a directory");
		}
		
		for (File srcFile : src.listFiles()) {
			File destFile = new File(dest, srcFile.getName());
			if (srcFile.isDirectory()) {
				destFile.mkdir();
				copyDirectories(srcFile, destFile);
			}
			else {
				try {
					Files.copy(srcFile, destFile);
				} catch (IOException e) {
					throw new RuntimeException("couldn't copy " + srcFile + " to " + destFile, e);
				}
			}
		}
	}
	
	public static List<Long> numericFilesInDirAsSortedLongList(File dir) {
        List<Long> fileNumbers;
		String[]	files;
		
        fileNumbers = new ArrayList<>();
	    files = dir.list();
        if (files != null) {
            for (String file : files) {
                try {
                    fileNumbers.add(Long.parseLong(file));
                } catch (NumberFormatException nfe) {
                    Log.info("Ignoring non-numeric file: ", file);
                }
            }
            Collections.sort(fileNumbers);
        }
        return fileNumbers;
	}
	
	public static List<Integer> numericFilesInDirAsSortedIntegerList(File dir) {
        List<Integer> fileNumbers;
		String[]	files;
		
        fileNumbers = new ArrayList<>();
	    files = dir.list();
        if (files != null) {
            for (String file : files) {
                try {
                    fileNumbers.add(Integer.parseInt(file));
                } catch (NumberFormatException nfe) {
                    Log.info("Ignoring non-numeric file: ", file);
                }
            }
            Collections.sort(fileNumbers);
        }
        return fileNumbers;
	}
}
