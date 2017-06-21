package com.ms.silverking.collection;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import com.ms.silverking.log.Log;

public class MapUtil {
	public enum NoDelimiterAction {Ignore, Warn, Exception};
	
	public static Map<String,String> parseStringMap(InputStream in, char delimiter, NoDelimiterAction noDelimiterAction) throws IOException {
		return parseMap(in, delimiter, noDelimiterAction, new StringParser(), new StringParser());
	}
	
	public static <K,V> Map<K,V> parseMap(InputStream in, char delimiter, NoDelimiterAction noDelimiterAction,
										Function<String,K> keyParser, Function<String,V> valueParser) throws IOException {
		BufferedReader	reader;
		String			line;
		int				lineNumber;
		Map<K,V>		map;
		
		map = new HashMap();
		lineNumber = 0;
		reader = new BufferedReader(new InputStreamReader(in));
		do {
			++lineNumber;
			line = reader.readLine();
			if (line != null) {
				int		dIndex;
				
				dIndex = line.indexOf(delimiter);
				if (dIndex >= 0) {
					String	kDef;
					String	vDef;
					
					kDef = line.substring(0, dIndex);
					vDef = line.substring(dIndex + 1);
					map.put(keyParser.apply(kDef), valueParser.apply(vDef));
				} else {
					switch (noDelimiterAction) {
					case Ignore: 
						break;
					case Warn:
						Log.warningf("No delimiter found on line %d", lineNumber);
						break;
					case Exception: throw new RuntimeException("No delimiter found on line: "+ lineNumber);
					default: throw new RuntimeException("Panic");
					}
				}
			}
		} while (line != null);
		return map;
	}
	
	private static class StringParser implements Function<String,String> {
		@Override
		public String apply(String s) {
			return s;
		}
	}
	
	public static void main(String[] args) {
		try {
			Map<String,String>	m;
			
			m = MapUtil.parseStringMap(new FileInputStream(args[0]), '\t', MapUtil.NoDelimiterAction.Warn);
			m.put("1","1");
			System.out.println(m);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
