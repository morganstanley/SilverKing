package com.ms.silverking.cloud.dht.client.gen;

import com.ms.silverking.text.StringUtil;

public class StatementParser {
	public static Statement parse(String s) {
		int	parenIndex;
		
		parenIndex = s.indexOf('(');
		if (parenIndex < 0) {
			return parseNonFunction(s);
		} else {
			return parseFunction(s);
		}
	}

	private static Statement parseNonFunction(String s) {
		LoopElement	loopElement;
		
		loopElement = LoopElement.parse(s);
		if (loopElement != null) {
			return loopElement;
		} else {
			return new Variable(s);
		}
	}

	private static Statement parseFunction(String s) {
		int			lParenIndex;
		int			rParenIndex;
		String		name;
		String[]	args;
		
		verifyOccurrences(s, '(', 1);
		verifyOccurrences(s, ')', 1);
		lParenIndex = s.indexOf('(');
		rParenIndex = s.indexOf(')');
		name = s.substring(0, lParenIndex);
		args = s.substring(lParenIndex + 1, rParenIndex).split(",");
		return new FunctionCall(name, args);
	}

	private static void verifyOccurrences(String s, char c, int i) {
		if (StringUtil.countOccurrences(s, c) != i) {
			throw new RuntimeException(String.format("\"%s\" does not have %d occurences", s, i));
		}
	}
}
