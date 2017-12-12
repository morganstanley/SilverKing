package com.ms.silverking.cloud.dht.client.gen;

import com.ms.silverking.text.StringUtil;

public class StatementParser {	
	private static final String	delimiterSuffix = "#";
	
	public static Statement parse(String s) {
		IfElement	ifElement;
		
		if (s.indexOf("IfNotLastElement") < 0) {
			ifElement = IfElement.parse(s);
		} else {
			ifElement = null;
		}
		if (ifElement != null) {
			return ifElement;
		} else {
			int	parenIndex;
			
			parenIndex = s.indexOf('(');
			if (parenIndex < 0 || s.startsWith(SwitchElement.Type.Switch.toString()) || s.startsWith(CaseElement._case)) {
				return parseNonFunction(s);
			} else {
				return parseFunction(s);
			}
		}
	}

	private static Statement parseNonFunction(String s) {
		LoopElement	loopElement;
		
		loopElement = LoopElement.parse(s);
		if (loopElement != null) {
			return loopElement;
		} else {
			IfElement	ifElement;
			
			ifElement = IfElement.parse(s);
			if (ifElement != null) {
				return ifElement;
			} else {
				SwitchElement	switchElement;
				
				switchElement = SwitchElement.parse(s);
				if (switchElement != null) {
					return switchElement;
				} else {
					CaseElement	caseElement;
					
					caseElement = CaseElement.parse(s);
					if (caseElement != null) {
						return caseElement;
					} else {
						return new Variable(s);
					}
				}
			}
		}
	}

	private static Statement parseFunction(String s) {
		int			lParenIndex;
		int			rParenIndex;
		String		name;
		String[]	args;
		String		delimiter;
				
		verifyOccurrences(s, '(', 1);
		verifyOccurrences(s, ')', 1);
		lParenIndex = s.indexOf('(');
		rParenIndex = s.indexOf(')');
		name = s.substring(0, lParenIndex);
		
		if (name.endsWith(delimiterSuffix)) {
			name = name.substring(0, name.length() - delimiterSuffix.length());
			delimiter = delimiterSuffix;
		} else {
			delimiter = ",";
		}		
		
		if (s.charAt(lParenIndex + 1) == '"' && s.charAt(rParenIndex - 1) == '"') {
			args = new String[1];
			args[0] = s.substring(lParenIndex + 2, rParenIndex - 1);
		} else {
			args = s.substring(lParenIndex + 1, rParenIndex).split(delimiter);
		}
		return new FunctionCall(name, args);
	}

	private static void verifyOccurrences(String s, char c, int i) {
		if (StringUtil.countOccurrences(s, c) != i) {
			throw new RuntimeException(String.format("\"%s\" does not have %d occurences", s, i));
		}
	}
}
