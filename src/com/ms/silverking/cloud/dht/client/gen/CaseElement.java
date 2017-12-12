package com.ms.silverking.cloud.dht.client.gen;

import com.ms.silverking.util.ArrayUtil;
import com.ms.silverking.util.Arrays;



public class CaseElement implements Statement {
	private final String[]	regexs;
	
	public static final String	_case = "Case";
	
	public CaseElement(String[] regexs) {
		this.regexs = regexs;
	}
	
	public static CaseElement parse(String s) {
		if (!s.startsWith(_case)) {
			return null;
		} else {
			String[]	regexs;			
			int			i0;
			int			i1;
			
			i0 = s.indexOf('(');
			if (i0 < 0) {
				throw new RuntimeException("Case without '('");
			}
			i1 = s.indexOf(')', i0);
			if (i1 < 0) {
				throw new RuntimeException("Case without matching ')'");
			}
			regexs = s.substring(i0 + 1, i1).split(",");
			return new CaseElement(regexs);
		}
	}
	
	public String getRegex() {
		return ArrayUtil.toString(regexs);
	}
	
	public boolean matches(String val) {
		for (String regex : regexs) {
			if (val.matches(regex)) {
				return true;
			}
		}
		return false;
	}

	public boolean isDefault() {
		return Arrays.contains(regexs, SwitchElement._default);
	}	
}
