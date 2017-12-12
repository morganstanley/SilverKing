package com.ms.silverking.cloud.dht.client.gen;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ms.silverking.text.StringUtil;

public class OldTypeMapCodeGenerator {
	private final Direction	direction;
	private final String	v0;
	private final String	v1;
	private final String	code;
	
	public enum Direction {skToJava, javaToSK};
	
	private static Pattern	    p = Pattern.compile("\\s*(\\w+)\\(([^,]+),([^\\)[\\*]]+)\\)\\s*\\{\\s*(\\S.*)");
	//private static Pattern	    p = Pattern.compile("\\s*(\\w+)\\((\\w+),(\\w+)\\)\\s*\\{\\s*(\\S.*)");
	
	public OldTypeMapCodeGenerator(Direction direction, String v0, String v1, String code) {
		this.direction = direction;
		this.v0 = v0;
		this.v1 = v1;
		this.code = code;
	}
	
	public String toString(String tabString, int tabDepth) {
		StringBuffer	sb;
		
		sb = new StringBuffer();
		sb.append(String.format("%s%s(%s,%s) {\n", StringUtil.replicate(tabString, tabDepth), direction, v0, v1));
		sb.append(String.format("%s%s\n", StringUtil.replicate(tabString, tabDepth + 1), code));
		sb.append(String.format("%s}\n", StringUtil.replicate(tabString, tabDepth)));
		return sb.toString();
	}
	
	@Override
	public String toString() {
		return toString(TypeMapping.tab, 0);
	}
	
	public static OldTypeMapCodeGenerator parse(String s) {
		Matcher	m;
		Direction direction;
		String v0;
		String v1;
		String code;
		
		s = s.trim();
		if (!s.endsWith("}")) {
			throw new RuntimeException("No terminating }");
		}
		s = s.substring(0, s.length() - 1);
		m = p.matcher(s);
		m.find();
		direction = Direction.valueOf(m.group(1));
		v0 = m.group(2);
		v1 = m.group(3);
		code = m.group(4);
		return new OldTypeMapCodeGenerator(direction, v0, v1, code);
	}
	
	public static void main(String[] args) {
		OldTypeMapCodeGenerator	t;
		
		t = new OldTypeMapCodeGenerator(OldTypeMapCodeGenerator.Direction.skToJava, "skm", "jm", "jm = skm.toJavaMap();");
		System.out.println(t.toString());
		System.out.println(parse(t.toString()));
	}
}
