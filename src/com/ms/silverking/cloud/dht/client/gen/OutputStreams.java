package com.ms.silverking.cloud.dht.client.gen;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

public class OutputStreams {
	private final Map<String,PrintStream>	outStreams;
	
	public OutputStreams() {
		outStreams = new HashMap<>();
	}
	
	public PrintStream getOutStream(String outputFileName) throws IOException {
		PrintStream	out;
		
		if (outputFileName == null) {
			throw new NullPointerException();
		}
		out = outStreams.get(outputFileName);
		if (out == null) {
			out = new PrintStream(new FileOutputStream(outputFileName));
			outStreams.put(outputFileName, out);
		} 
		return out;
	}

	public void closeAll() {
		for (PrintStream out : outStreams.values()) {
			out.close();
		}
	}
	
	public void print(Context c, Text t) throws IOException {
		getOutStream(c.getOutputDir().getPath() +"/"+ c.getOutputFileName()).print(t.toString());
	}
	
	public void print(Context c, String s) throws IOException {
		getOutStream(c.getOutputDir().getPath() +"/"+ c.getOutputFileName()).print(s);
	}
}
