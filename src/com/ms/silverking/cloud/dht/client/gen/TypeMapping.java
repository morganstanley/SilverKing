package com.ms.silverking.cloud.dht.client.gen;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ms.silverking.collection.Triple;
import com.ms.silverking.io.FileUtil;
import com.ms.silverking.text.StringUtil;

public class TypeMapping {
	private final String	javaType;
	private final String	externalType;
	private final TypeMapCodeGenerator	skToJavaGenerator;
	private final TypeMapCodeGenerator	javaToSKGenerator;
	
	private static final String	mapString = "MAP";
	private static final String	javaTypeString = "JAVA_TYPE";
	
	static final String	tab = "    ";
	
	//private static Pattern	    p = Pattern.compile("\\s*"+mapString+"\\(([\\w[.]]+),([\\w[.]]+)\\)\\s*\\{\\s*([^\\}]*)\\}\\s*([^\\}]*)\\}");
	private static Pattern	    p = Pattern.compile("\\s*"+mapString+"\\(([\\w[.]]+),([\\w[.]]+)\\)\\s*\\{\\s*(.*;)\\s*(.*;)\\s*\\}");
	
	public TypeMapping(String javaType, String externalType, TypeMapCodeGenerator skToJavaGenerator, TypeMapCodeGenerator javaToSKGenerator) {
		this.javaType = javaType;
		this.externalType = externalType;
		this.skToJavaGenerator = skToJavaGenerator;
		this.javaToSKGenerator = javaToSKGenerator;
	}
	
	public String getJavaType() {
		return javaType;
	}

	public String getExternalType() {
		return externalType;
	}

	public TypeMapCodeGenerator getSKToJavaGenerator() {
		return skToJavaGenerator;
	}

	public TypeMapCodeGenerator getJavaToSKGenerator() {
		return javaToSKGenerator;
	}

	
	public String toString(String tabString, int tabDepth) {
		StringBuffer	sb;
		
		sb = new StringBuffer();
		sb.append(String.format("%s%s(%s,%s) {\n", StringUtil.replicate(tabString, tabDepth), mapString, javaType, externalType));
		sb.append(skToJavaGenerator.toString(tab, tabDepth + 1));
		sb.append(javaToSKGenerator.toString(tab, tabDepth + 1));
		sb.append(String.format("%s}\n", StringUtil.replicate(tabString, tabDepth)));
		return sb.toString();
	}	
	
	@Override
	public String toString() {
		return toString(tab, 0);
	}
	
	public static TypeMapping parse(String s) {
		try {
			Matcher	m;
			String	javaType;
			String	externalType;
			TypeMapCodeGenerator	skToJavaGenerator;
			TypeMapCodeGenerator	javaToSKGenerator;
			Triple<String,Integer,Integer>	jt;
			
			jt = javaType(s);
			s = s.substring(0, jt.getV2()) + javaTypeString + s.substring(jt.getV3());
			
			m = p.matcher(s);
			m.find();
			//javaType = m.group(1);
			javaType = jt.getV1();
			externalType = m.group(2);
			skToJavaGenerator = TypeMapCodeGenerator.parse(m.group(3) +"}");
			javaToSKGenerator = TypeMapCodeGenerator.parse(m.group(4) +"}");
			return new TypeMapping(javaType, externalType, skToJavaGenerator, javaToSKGenerator);
		} catch (RuntimeException re) {
			System.out.println(s);
			throw re;
		}
	}
	
	private static Triple<String,Integer,Integer> javaType(String s) {
		int	i0;
		int	i1;
		int	i2;
		
		i0 = s.indexOf(mapString);
		if (i0 < 0) {
			throw new RuntimeException("Invalid mapping: "+ s);
		}
		i1 = s.indexOf("(", i0);
		if (i1 < 0) {
			throw new RuntimeException("Invalid mapping: "+ s);
		}
		i2 = findCommaOutsideOfBrackets(s, i1);
		if (i2 < 0) {
			throw new RuntimeException("Invalid mapping: "+ s);
		}
		return new Triple<>(s.substring(i1 + 1, i2).trim(), i1 + 1, i2);
	}
	
	private static int findCommaOutsideOfBrackets(String s, int i0) {
		int	depth;
		
		depth = 0;
		for (int i = i0; i < s.length(); i++) {
			char	c;
			
			c = s.charAt(i);
			if (c == '<') {
				depth++;
			} else if (c == '>') {
				depth--;
				if (depth < 0) {
					throw new RuntimeException("Invalid mapping: "+ s);
				}
			} else if (c == ',') {
				if (depth == 0) {
					return i;
				}
			}
		}
		return -1;
	}

	public static List<TypeMapping> readTypeMappings(String typeMappingFile) throws IOException {
		List<TypeMapping>	tmList;
		
		tmList = new ArrayList<>();
		for (String def : FileUtil.readFileAsString(typeMappingFile).split(mapString)) {
			def = def.trim();
			if (def.length() > 0) {
				tmList.add(TypeMapping.parse(mapString + def));
			}
		}
		return tmList;
	}
	
	public static void main(String[] args) {
		TypeMapping	tm;
		
		tm = new TypeMapping("java.util.Map", "SKMap", 
				new TypeMapCodeGenerator(TypeMapCodeGenerator.Direction.skToJava, "skm", "jm = skm.toJavaMap()"),
				new TypeMapCodeGenerator(TypeMapCodeGenerator.Direction.javaToSK, "jm", "skm = SKMap::fromJavaMap(jm)"));
		System.out.println(tm.toString());
		System.out.println(parse(tm.toString()));
	}
}
