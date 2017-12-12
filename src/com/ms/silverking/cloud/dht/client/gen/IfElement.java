package com.ms.silverking.cloud.dht.client.gen;

import java.util.List;

import com.ms.silverking.collection.EnumUtil;
import com.ms.silverking.collection.Triple;

public class IfElement implements Statement {
	private final Type		type;
	private final Condition	condition;
	private final Variable	variable;
	private final String	literal;
	
	public enum Type {If,Else,EndIf};
	public enum Condition {Equal,NotEqual, None};
	
	public IfElement(Type type, Condition condition, Variable variable, String literal) {
		this.type = type;
		this.condition = condition;
		this.variable = variable;
		this.literal = literal;
	}
	
	public static IfElement parse(String s) {
		Type	t;
		
		//System.out.printf("\n*** %s\n", s);
		s = s.trim();
		t = EnumUtil.getEnumFromStringStart(Type.class, s);
		if (t == null) {
			return null;
		} else {
			Condition	c;
			Variable	v;
			String		l;
			
			if (t == Type.If) {
				String		ps;
				int			i0;
				int			i1;
				String[]	params;
				
				i0 = s.indexOf('(');
				if (i0 < 0) {
					throw new RuntimeException("If without '('");
				}
				i1 = s.indexOf(')', i0);
				if (i1 < 0) {
					throw new RuntimeException("If without matching ')'");
				}
				ps = s.substring(i0 + 1, i1);
				params = ps.split(",");
				if (params.length != 2) {
					throw new RuntimeException(String.format("Incorrect number of If parameters. Expected 2. Found %d", params.length));
				}
				c = Condition.valueOf(s.substring("If".length(), i0).trim());
				v = new Variable(params[0].trim());
				l = params[1].trim();
			} else {
				c = Condition.None;
				v = null;
				l = null;
			}
			return new IfElement(t, c, v, l);
		}
	}
	
	public Type getType() {
		return type;
	}
	
	public Condition getCondition() {
		return condition;
	}

	public Variable getVariable() {
		return variable;
	}

	public String getLiteral() {
		return literal;
	}
	
	private boolean evaluateVariable(Context c) {
		return variable.evaluate(c).getV2().equals(literal);
	}
	
	public static List<ParseElement> evaluate(Context c, Triple<IfElement,List<ParseElement>,List<ParseElement>> _if) {
		boolean	rawEval;
		boolean	useThen;
		
		rawEval = _if.getV1().evaluateVariable(c);
		switch (_if.getV1().getCondition()) {
		case Equal: useThen = rawEval; break;
		case NotEqual: useThen = !rawEval; break;
		default: throw new RuntimeException("Panic");
		}
		return useThen ? _if.getV2() : _if.getV3();
	}
}
