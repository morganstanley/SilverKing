package com.ms.silverking.cloud.skfs.analysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ms.silverking.collection.HashedListMap;

public class FileCreationTimeAnalysis {
	private final HashedListMap<String,Double>	creationTimes;
	
	private static final int	timeIndex = 1;
	private static final int	opIndex = 3;
	private static final int	fileNameIndex = 4;
	
	public FileCreationTimeAnalysis() {
		creationTimes = new HashedListMap<>();
	}
	
	public void analyze(String s) throws IOException {
		analyze(new File(s));
	}
	
	public void analyze(File f) throws IOException {
		analyze(new FileInputStream(f));
	}
	
	public void analyze(InputStream in) throws IOException {
		BufferedReader	reader;
		String			line;
		Map<String,HMS>	openTimes;
		
		openTimes = new HashMap<>();
		reader = new BufferedReader(new InputStreamReader(in));
		do {
			line = reader.readLine();
			if (line != null) {
				processLine(line, openTimes);
			}
		} while (line != null);
	}

	private void processLine(String line, Map<String,HMS> openTimes) {
		String[]	tok;
		HMS			time;
		String		fileName;
		
		tok = line.split("\\s+");
		time = new HMS(tok[timeIndex]);
		fileName = tok[fileNameIndex];
		if (tok[opIndex].equals("_o")) {
			HMS	prev;
			
			prev = openTimes.put(fileName, time);
			if (prev != null) {
				System.out.printf("Warning replaced: %s\n", fileName);
			}
		} else if (tok[opIndex].equals("_f")) {
			HMS	openTime;
			
			openTime = openTimes.get(fileName);
			if (openTime == null) {
				System.out.printf("Ignoring (no open time): %s\n", line);
			} else {
				double	delta;
				
				delta = HMS.deltaSeconds(openTime, time);
				creationTimes.addValue(fileName, delta);
			}
		} else {
			System.out.printf("Ignoring (unknown op): %s\n", line);
		}
	}
	
	public void display() {
		for (String fileName : creationTimes.keySet()) {
			List<Double> times;
			
			times = creationTimes.getList(fileName);
			for (Double time : times) {
				System.out.printf("%s\t%f\n", fileName, time);
			}
		}
	}
	
	private static class HMS {
		final byte		hours;
		final byte		minutes;
		final double	seconds;
		
		public HMS(String s) {
			String[]	t;
			
			t = s.split(":");
			hours = Byte.parseByte(t[0]);
			minutes = Byte.parseByte(t[1]);
			seconds = Double.parseDouble(t[2]);
		}
		
		static double deltaSeconds(HMS t0, HMS t1) {
			return ((t1.hours - t0.hours) * 60 + (t1.minutes - t0.minutes)) * 60.0 + (t1.seconds - t0.seconds);
		}
	}

	public static void main(String[] args) {
		try {
			if (args.length == 0) {
				System.out.println("args: <file...>");
			} else {
				FileCreationTimeAnalysis	fa;
				
				fa = new FileCreationTimeAnalysis();
				for (String f : args) {
					fa.analyze(f);
				}
				fa.display();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
