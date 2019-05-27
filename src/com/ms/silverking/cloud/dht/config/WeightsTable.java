package com.ms.silverking.cloud.dht.config;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class WeightsTable {
	private final Map<String, Double>	nodeWeights;
	
	public WeightsTable(Map<String, Double>	nodeWeights) {
		this.nodeWeights = nodeWeights;
	}
	
	public static WeightsTable parse(File fileName) throws IOException {
		return parse(new FileInputStream(fileName));
	}
	
	public static WeightsTable parse(InputStream inStream) throws IOException {
		try {
			BufferedReader	reader;
			String			line;
			Map<String, Double>	nodeWeights;
			
			nodeWeights = new HashMap<String, Double>();
			reader = new BufferedReader(new InputStreamReader(inStream));
			do {				
				line = reader.readLine();
				if (line != null) {
					line = line.trim();
					if (line.length() != 0) {
						String[]	tokens;
						
						tokens = line.split("\\s+");
						if (tokens.length != 2) {
							throw new IOException("Invalid weight line: "+ line);
						} else {
							nodeWeights.put(tokens[0], Double.parseDouble(tokens[1]));
						}
					}
				}
			} while (line != null);
			return new WeightsTable(nodeWeights);
		} finally {
			inStream.close();
		}
	}
	
	public void display() {
		for (Map.Entry<String, Double> entry : nodeWeights.entrySet()) {
			System.out.println(entry.getKey() +"\t"+ entry.getValue());
		}
	}
	
	// for unit testing
	public static void main(String[] args) {
		try {
			if (args.length != 1) {
				System.err.println("<file>");
			} else {
				WeightsTable	table;
				
				table = WeightsTable.parse(new File(args[0]));
				table.display();
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
}
