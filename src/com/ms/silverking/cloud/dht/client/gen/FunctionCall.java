package com.ms.silverking.cloud.dht.client.gen;

import java.io.IOException;

import com.ms.silverking.collection.Pair;
import com.ms.silverking.io.FileUtil;
import com.ms.silverking.util.ArrayUtil;

public class FunctionCall implements Expression {
	private final String	name;
	private final String[]	args;
	
	public FunctionCall(String name, String[] args) {
		this.name = name;
		this.args = args;
	}

	@Override
	public Pair<Context,String> evaluate(Context c) {
		String[]	resolvedArgs;
		
		resolvedArgs = resolve(c, args);
		switch (name) {
		case "Replace": return doReplace(c, resolvedArgs);
		case "SetOutputFile": return doSetOutputFile(c, resolvedArgs);
		case "SetEnclosingTypeSeparator": return doSetEnclosingTypeSeparator(c, resolvedArgs);
		case "Option": return doOption(c, resolvedArgs);
		case "Include": return doInclude(c, resolvedArgs);
		case "IfNotLastElement": return doIfNotLastElement(c, resolvedArgs);
		//case "ReturnType": return returnType(c, resolvedArgs);
		default: throw new RuntimeException("Unknown function: "+ name);
		}
	}
	
	private String[] resolve(Context c, String[] args) {
		String[]	resolvedArgs;
		
		resolvedArgs = new String[args.length];
		for (int i = 0; i < resolvedArgs.length; i++) {
			if (Variable.isVariable(args[i])) {
				resolvedArgs[i] = new Variable(args[i]).evaluate(c).getV2();
			} else {
				resolvedArgs[i] = args[i];
			}
			//System.out.printf("%s\t%s\t%s\n", args[i], Variable.isVariable(args[i]), resolvedArgs[i]);
		}
		return resolvedArgs;
	}

	private Pair<Context,String> doReplace(Context c, String[] resolvedArgs) {
		// (variable name, regex, replacement)
		//System.out.printf("%s\t%s\t%s\n", resolvedArgs[0], resolvedArgs[1], resolvedArgs[2]);
		return new Pair<>(c, resolvedArgs[0].replaceAll(resolvedArgs[1], resolvedArgs[2]));
	}

	private Pair<Context,String> doSetOutputFile(Context c, String[] resolvedArgs) {
		// (variable name, suffix)
		return new Pair<>(c.outputFileName(resolvedArgs[0] + resolvedArgs[1]), null);
	}
	
	private Pair<Context,String> doSetEnclosingTypeSeparator(Context c, String[] resolvedArgs) {
		// (variable name, suffix)
		return new Pair<>(c.enclosingTypeSeparator(resolvedArgs[0]), null);
	}
	
	private Pair<Context,String> doOption(Context c, String[] resolvedArgs) {
		System.out.printf("\t\tdoOption %s\n", ArrayUtil.toString(resolvedArgs));
		if (resolvedArgs[1].equals("null")) {
			// (optionValue, notNullOption, nullOption)
			return new Pair<>(c, resolvedArgs[0] != null ? resolvedArgs[2] : resolvedArgs[3]);
		} else {
			// (optionValue, notNullOption, nullOption)
			return new Pair<>(c, resolvedArgs[0].equals(resolvedArgs[1]) ? resolvedArgs[2] : resolvedArgs[3]);
		}
	}
	
	private Pair<Context,String> doInclude(Context c, String[] resolvedArgs) {
		try {
			// (includeFile)
			return new Pair<>(c, FileUtil.readFileAsString(resolvedArgs[0]));
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
	}
	
	private Pair<Context,String> doIfNotLastElement(Context c, String[] resolvedArgs) {
		// (includeFile)
		//return new Pair<>(c, c.getLoopIndex() +"_"+ c.getLoopElements());
		return new Pair<>(c, c.isLastLoopElement() ? "" : resolvedArgs[0]);
	}
	
	/*
	private Pair<Context,String> returnType(Context c, String[] resolvedArgs) {
		// (includeFile)
		//return new Pair<>(c, c.getLoopIndex() +"_"+ c.getLoopElements());
		return new Pair<>(c, Variable.getReturnTypeWrapped(c.getMethod(), resolvedArgs[0]));
	}
	*/
}
