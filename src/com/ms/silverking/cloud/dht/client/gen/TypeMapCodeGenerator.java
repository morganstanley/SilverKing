package com.ms.silverking.cloud.dht.client.gen;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ms.silverking.text.StringUtil;

public class TypeMapCodeGenerator {
	private final Direction	direction;
	private final String	v;
	private final String	code;
	
	public enum Direction {skToJava, javaToSK};
	
	private static Pattern	    p = Pattern.compile("\\s*(\\w+)\\(([^,]+)\\)\\s*(\\S.*);");
	
	public TypeMapCodeGenerator(Direction direction, String v, String code) {
		this.direction = direction;
		this.v = v;
		this.code = code;
	}
	
	public String generate(String replacementText) {
		return code.replaceAll(v, replacementText);
	}
	
	public String getReturnWrapper() {
		int	index;
		
		System.out.printf(">>>%s", code);
		index = code.indexOf('(');
		if (index < 0) {
			return null;
		} else {
			return code.substring(0, index);
		}
	}
	
	public String toString(String tabString, int tabDepth) {
		StringBuffer	sb;
		
		sb = new StringBuffer();
		sb.append(String.format("%s%s(%s)%s%s;", StringUtil.replicate(tabString, tabDepth), direction, v, tabString, code));
		return sb.toString();
	}
	
	@Override
	public String toString() {
		return toString(TypeMapping.tab, 0);
	}
	
	public static TypeMapCodeGenerator parse(String s) {
		Matcher	m;
		Direction direction;
		String v;
		String code;
		
		s = s.trim();
		m = p.matcher(s);
		m.find();
		direction = Direction.valueOf(m.group(1));
		v = m.group(2);
		code = m.group(3);
		return new TypeMapCodeGenerator(direction, v, code);
	}
	
	public static void main(String[] args) {
		TypeMapCodeGenerator	t;
		
		t = new TypeMapCodeGenerator(TypeMapCodeGenerator.Direction.skToJava, "skm", "skm.toJavaMap()");
		System.out.println(t.toString());
		System.out.println(parse(t.toString()));
	}
}
