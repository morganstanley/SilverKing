package com.ms.silverking.cloud.dht.client.gen;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.ms.silverking.collection.EnumUtil;
import com.ms.silverking.collection.Pair;

public class SwitchElement implements Statement {
	private final Type		type;
	private final Variable	variable;
	
	public enum Type {Switch,EndSwitch};
	
	public static final String	_default = "default";
	
	private static final boolean	debug = true;
	
	public SwitchElement(Type type, Variable variable) {
		this.type = type;
		this.variable = variable;
	}
	
	public static SwitchElement parse(String s) {
		Type	t;
		
		//System.out.printf("\n*** %s\n", s);
		s = s.trim();
		t = EnumUtil.getEnumFromStringStart(Type.class, s);
		if (t == null) {
			return null;
		} else {
			Variable	v;
			
			if (t == Type.Switch) {
				String		ps;
				int			i0;
				int			i1;
				
				i0 = s.indexOf('(');
				if (i0 < 0) {
					throw new RuntimeException("Switch without '('");
				}
				i1 = s.indexOf(')', i0);
				if (i1 < 0) {
					throw new RuntimeException("Switch without matching ')'");
				}
				ps = s.substring(i0 + 1, i1);
				v = new Variable(ps.trim());
			} else {
				v = null;
			}
			return new SwitchElement(t, v);
		}
	}
	
	public Type getType() {
		return type;
	}
	
	public Variable getVariable() {
		return variable;
	}

	public static List<ParseElement> evaluate(Context c, Pair<SwitchElement, List<Pair<CaseElement, List<ParseElement>>>> _switch) {
		String	value;
		Pair<CaseElement, List<ParseElement>>	defaultCase;

		if (debug) {
			System.out.printf("SwitchElement.evaluate() %d\n", _switch.getV2().size());
		}
		defaultCase = null;
		value = _switch.getV1().getVariable().evaluate(c).getV2();
		if (debug) {
			System.out.printf("SwitchElement.evaluate() variable %s\n", _switch.getV1().getVariable().getType());
			System.out.printf("SwitchElement.evaluate() value %s\n", value);
		}
		for (Pair<CaseElement, List<ParseElement>>	_case : _switch.getV2()) {
			if (_case.getV1().isDefault()) {
				if (defaultCase != null) {
					throw new RuntimeException("Multiple default cases in switch");
				} else {
					defaultCase = _case;
				}
			} else {
				if (_case.getV1().matches(value)) {
					if (debug) {
						System.out.printf("Match case %s value %s\n", _case.getV1().getRegex(), value);
					}
					return _case.getV2();
				} else {
					if (debug) {
						System.out.printf("No switch match case %s value %s\n", _case.getV1().getRegex(), value);
					}
				}
			}
		}
		if (defaultCase != null) {
			return defaultCase.getV2();
		} else {
			return ImmutableList.of();
		}
	}
}
