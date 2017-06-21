package com.ms.silverking.cloud.dht.client.gen;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.ms.silverking.cloud.dht.client.gen.LoopElement.Target;
import com.ms.silverking.io.StreamUtil;
import com.ms.silverking.text.StringUtil;

public class WrapperGenerator {
	/*
	private final WrapperGeneratorOptions	options;
	
	private enum ParseState {OutsideExpression, InsideExpression};
	
	private static final boolean	debugParse = true;
	
	public WrapperGenerator(WrapperGeneratorOptions options) {
		this.options = options;
	}
	
	public void generate(List<GenPackageClasses> packageClassesList, File templateFile, File outputDir) throws IOException {
		for (GenPackageClasses packageClasses : packageClassesList) {
			generate(packageClasses, templateFile, outputDir);
		}
	}
	
	private void generate(GenPackageClasses packageClasses, File templateFile, File outputDir) throws IOException {
		List<ParseElement>	elements;
		Context	c;
		
		c = new Context();
		elements = parseElements(templateFile);
		generate(c, elements, outputDir);
		c.close();
	}

	// Loop through all elements
	// For text, put as is
	// for package loops put in
	private void generate(Context c, List<ParseElement> elements, File outputDir) throws IOException {
		int	i;
		
		i = 0;
		while (i < elements.size()) {
			ParseElement	e;
			
			e = elements.get(i);
			if (e instanceof Text) {
				c.getOutStream().print(e);
				i++;
			} else if (e instanceof Expression) {
				((Expression)e).evaluate(c);
				i++;
			} else if (e instanceof LoopElement) {
				LoopElement	le;
				
				le = (LoopElement)e;
				if (!c.isValidTarget(le.getTarget())) {
					generateLoopTargetException(le, LoopElement.Target.Classes);
				} else {
					List<ParseElement> loopElements;
					
					loopElements = getLoopElements(elements, i);
					switch (le.getTarget()) {
					case Classes:
						for (Class _class : packageClasses.getClasses()) {
							Context	_c;
							
							_c = null;
							generate(c, loopElements, outputDir);
						}
						break;
						default: throw new RuntimeException("Panic");
					}
					i += loopElements.size() + 2;
				}
			} else {
				throw new RuntimeException("Panic");
			}
		}
	}
	
	private List<ParseElement> getLoopElements(List<ParseElement> elements, int le0Index) {
		List<ParseElement>	loopElements;
		LoopElement			le0;
		
		le0 = loopElements.get(le0Index);
		loopElements = new ArrayList<>();
		// simplistic, doesn't support nesting
		for (int i = le0Index + 1; i < elements.size(); i++) {
			ParseElement	e;
			
			e = elements.get(i);
			if (e instanceof LoopElement) {
				LoopElement	le2;
				
				le2 = (LoopElement)e;
				if (le2.getTarget().equals(le.getTarget())) {
					return loopElements;
				}
			}
		}
		throw new RuntimeException(String.format("No loop end for %s", le));
	}

	private void generateLoopTargetException(LoopElement le, Target target) {
		throw new RuntimeException(String.format("Invalid target for %s. Expected %s.", le, target));
	}

	public void generate(File templateFile, File outputDir) throws IOException {
		if (!outputDir.exists()) {
			throw new RuntimeException("outputDir does not exist: "+ outputDir);
		} else {
			List<ParseElement>	elements;
			
			elements = parseElements(templateFile);
			displayElements(elements);
		}
	}
	
	private void generate(List<ParseElement> elements, Context c) {
		for (int i = 0; i < elements.size(); i++) {
			ParseElement	e;
			
			e = elements.get(i);
			if (e instanceof Text) {
				System.out.print(e);
			} else if (e instanceof Expression) {
				((Expression)e).evaluate(c);
			} else if (e instanceof LoopElement) {
				LoopElement	le;
				
				le = (LoopElement)e;
				loop(le, getEnclosedStatements(elements, i), c);
				generate(getEnclosedStatements(elements, i), c);
				//generate(getEndingLoopElementIndex(le, elements)
			} else {
				throw new RuntimeException("Panic");
			}
		}
	}
	
	private void loop(LoopElement e, List<ParseElement> enclosedStatements, Context c) {
	}

	private List<ParseElement> getEnclosedStatements(List<ParseElement> elements, int i) {
		return null;
	}

	////////////////////////////////////////////////////////////////////
	
	private void displayElements(List<ParseElement> elements) {
		for (int i = 0; i < elements.size(); i++) {
			System.out.printf("%d: %s\n\n", i, elements.get(i));
		}
	}
	
	////////////////////////////////////////////////////////////////////
	
	private int nextDelimiter(String s, String d, int i) {
		int	next;
		
		next = s.indexOf(d, i);
		if (next < 0) {
			next = s.length();
		}
		return next;
	}
	
	private void generateParseException(String s, String message, int location) {
		int	line;
		
		line = StringUtil.countOccurrences(s, '\n', location) + 1;
		throw new RuntimeException(String.format("Line: %d. %s", line, message));
	}

	private List<ParseElement> parseElements(File templateFile) throws IOException {
		String	s;
		byte[]	buf;
		int		nextStart;
		int		nextEnd;
		int		i;
		int		length;
		ParseState	state;
		List<ParseElement>	elements;
		
		elements = new ArrayList<>();
		length = Math.toIntExact(templateFile.length());
		buf = new byte[length];
		StreamUtil.readFully(new FileInputStream(templateFile), buf);
		s = new String(buf);
		i = 0;
		state = ParseState.OutsideExpression;
		while (i < length) {
			nextStart = nextDelimiter(s, options.startDelimiter, i);
			nextEnd = nextDelimiter(s, options.endDelimiter, i);			
			if (debugParse) {
				System.out.printf("%d %d %s %d %d\n", i, (StringUtil.countOccurrences(s, '\n', i) + 1), state, nextStart, nextEnd);
			}
			switch (state) {
			case OutsideExpression:
				if (nextStart > i) {
					if (nextEnd < nextStart) {
						generateParseException(s, "Unexpected "+ options.endDelimiter, i);
					}
					elements.add(new Text(s.substring(i, nextStart)));
				}
				i = nextStart + options.startDelimiter.length();
				state = ParseState.InsideExpression;
				break;
			case InsideExpression:
				if (nextStart < nextEnd) {
					generateParseException(s, "Unexpected "+ options.startDelimiter, i);
				}
				if (nextEnd == length) {
					generateParseException(s, "Missing "+ options.endDelimiter, i);
				}
				elements.add(StatementParser.parse(s.substring(i, nextEnd)));
				i = nextEnd + options.endDelimiter.length();
				state = ParseState.OutsideExpression;
				break;
			default: throw new RuntimeException("panic");
			}
		}
		return elements;
	}
	
	public static void main(String[] args) {
    	try {
    		WrapperGenerator		wg;
    		WrapperGeneratorOptions	options;
    		CmdLineParser			parser;
    		
    		options = new WrapperGeneratorOptions();
    		parser = new CmdLineParser(options);
    		try {
    			parser.parseArgument(args);
    		} catch (CmdLineException cle) {
    			System.err.println(cle.getMessage());
    			parser.printUsage(System.err);
    			return;
    		}
    		wg = new WrapperGenerator(options);
    		wg.generate(GenPackageClasses.createForPackagesAndClasses(new File(options.codebase), options.inputPackagesAndClasses), new File(options.templateFile), new File (options.outputDir));
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
	}
	*/
}
