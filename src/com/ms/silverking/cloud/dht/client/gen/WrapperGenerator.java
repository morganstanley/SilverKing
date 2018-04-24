package com.ms.silverking.cloud.dht.client.gen;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.client.gen.LoopElement.Target;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.collection.Quadruple;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.io.StreamUtil;
import com.ms.silverking.text.StringUtil;

public class WrapperGenerator {
	private final WrapperGeneratorOptions	options;
	private final OutputStreams				outStreams;
	private ClassReferenceFinder		referenceFinder; 
	
	private enum ParseState {OutsideExpression, InsideExpression};
	
	private static final String	debugTab = "  ";
	
	private static final boolean	debugParse = false;
	private static final boolean	debugGeneration = false;
	private static final boolean	debugSwitchElements = false;
	
	private static final Set<String>	methodsToOmit;
	private static final Set<Class>		baseClasses;
	
	static {
		Set<String>	o;
		
		o = new HashSet<>();
		for (Method m : Object.class.getDeclaredMethods()) {
			o.add(m.getName());
		}
		o.add("values"); // For enum types
		o.add("valueOf"); // For enum types. FIXME - put this back in once we can convert the string option correctly 
		o.add("compareTo"); // Comparable brings in an object method that we don't want
		methodsToOmit = ImmutableSet.copyOf(o);
		System.out.printf("methodsToOmit:\n%s\n", CollectionUtil.toString(methodsToOmit, '\n'));
		baseClasses = new HashSet<>();
		baseClasses.add(Object.class);
		baseClasses.add(Enum.class);
		baseClasses.add(Exception.class);
	}
	
	public WrapperGenerator(WrapperGeneratorOptions options) {
		this.options = options;
		outStreams = new OutputStreams();
	}
		
	/**
	 * Loop through all given packages and generate code for the specified classes in those packages
	 * @param packageClassesList
	 * @param templateFile
	 * @param outputDir
	 * @throws IOException
	 */
	private void generate(List<GenPackageClasses> packageClassesList, File templateFile, File outputDir, boolean addDependencies) throws IOException {
		List<ParseElement>	elements;
		Context	c;
		
		referenceFinder = new ClassReferenceFinder(packageClassesList);
		referenceFinder.addReferencedClasses();
		if (addDependencies) {
			packageClassesList = GenPackageClasses.add(packageClassesList, referenceFinder.getReferences(), new File(options.codebase));
		}

		System.out.println("**********************************************");
		System.out.println("**********************************************");
		for (GenPackageClasses gpc : packageClassesList) {
			System.out.println(gpc);
			for (Class _c : gpc.getClasses()) {
				System.out.println(_c);
			}
		}
		
		c = new Context(packageClassesList, outputDir);
		elements = parseElements(templateFile);
		//displayElements(elements);
		elements = filterExtraneousNewlines(elements);
		generate(c, elements, outputDir, 0, 0);
	}

	private List<ParseElement> filterExtraneousNewlines(List<ParseElement> elements) {
		List<ParseElement>	filtered;
		ParseElement	p1;
		ParseElement	p0;
		
		filtered = new ArrayList<>();
		p1 = null;
		p0 = null;
		for (ParseElement e : elements) {
			ParseElement	add;
			
			add = e;
			if (p1 != null && p0 != null) {
				/*
				if ((p1 instanceof Text) && (p0 instanceof LoopElement) && (e instanceof Text)) {
					Text	t1;
					Text	te;
					
					t1 = (Text)p1;
					te = (Text)e;
					System.out.printf("1//%s\\\\ 2//%s\\\\\n", t1, te);
					if (t1.toString().contains("\n") || t1.toString().contains("\r")) {
						if (te.toString().startsWith("\n") || te.toString().startsWith("\r")) {
							add = new Text(te.toString().substring(1));
						}						
					}
				}
				*/
				if ((p0 instanceof LoopElement) && (e instanceof Text)) {
					Text	te;
					
					te = (Text)e;
					if (te.toString().startsWith("\n") || te.toString().startsWith("\r")) {
						int		index;
						
						if (te.toString().length() > 1) {
							char	c2;
							
							c2 = te.toString().charAt(1);
							if (c2 == '\n' || c2 == '\r') {
								index = 2;
							} else {
								index = 1;
							}
						} else {
							index = 1;
						}
						add = new Text(te.toString().substring(index));
					}
					/*
					System.out.printf("%x::%s", (int)te.toString().charAt(0), te);
					System.out.printf("%x##%s", (int)add.toString().charAt(0), add);
					System.out.println();
					*/
				}
			}
			filtered.add(add);
			p1 = p0;
			p0 = e;
		}
		return filtered;
	}
	
	private Method[] getMethodsForGeneration(Class c) {
		List<Method>	methods;
		
		methods = new ArrayList<>();
		for (Method m : c.getDeclaredMethods()) {
			if (!methodsToOmit.contains(m.getName()) && !m.isBridge()) {
				methods.add(m);
			}
		}
		/*
		try {
			Class[]	t;
			
			t = new Class[1];
			t[0] = void.class;
			methods.add(c.getMethod("toString", null));
		} catch (NoSuchMethodException | SecurityException e) {
			throw new RuntimeException("Panic", e);
		}
		*/
		return methods.toArray(new Method[0]);
	}
	
	private Field[] getStaticFieldsForGeneration(Class c) {
		List<Field>	fields;
		
		fields= new ArrayList<>();
		for (Field f : c.getFields()) {
			if (Modifier.isPublic(f.getModifiers()) && Modifier.isStatic(f.getModifiers()) && Modifier.isFinal(f.getModifiers())) {
				fields.add(f);
			}
		}
		return fields.toArray(new Field[0]);
	}
	
	private static boolean isBaseClass(Class c) {
		return baseClasses.contains(c);
	}
	
	/**
	 * Loop through all ParseElements in the list. 
	 * For text, put as is.
	 * For other elements, process according to type.
	 * @param c
	 * @param elements
	 * @param outputDir
	 * @param loopIndex
	 * @param depth TODO
	 * @throws IOException
	 */
	private void generate(Context c, List<ParseElement> elements, File outputDir, int loopIndex, int depth) throws IOException {
		int	i;
		
		if (debugGeneration) {
			Thread.dumpStack();
			System.out.printf("generate elements %d loopIndex %d depth %d\n", elements.size(), loopIndex, depth);
		}
		i = 0;
		while (i < elements.size()) {
			ParseElement	e;
			
			e = elements.get(i);
			c = c.loopIndex(loopIndex);
			if (debugGeneration) {
				System.out.printf("%s%d\t%s\n", StringUtil.replicate(debugTab, depth), i, e);
			}
			if (e instanceof Text) {
				outStreams.print(c, (Text)e);
				i++;
			} else if (e instanceof Expression) {
				Pair<Context,String>	result;
				
				result = ((Expression)e).evaluate(c);
				c = result.getV1();
				if (result.getV2() != null) {
					outStreams.print(c, result.getV2());
				}
				i++;
			} else if (e instanceof LoopElement) {
				LoopElement	le;
				
				le = (LoopElement)e;
				if (!isValidLoopTarget(c.getLoopElement(), le.getTarget())) {
					generateLoopTargetException(le);
				} else {
					List<ParseElement> loopElements;
					Context	_c;
					int	newLoopIndex;
					
					newLoopIndex = 0;
					loopElements = getLoopElements(elements, i);
					_c = c.loopElement(le);
					if (debugGeneration) {
						System.out.printf("G%sLoop %d %d\n", StringUtil.replicate(debugTab, depth), i, i + loopElements.size());
					}
					switch (le.getTarget()) {
					case Packages:
						for (GenPackageClasses genPackageClasses : c.getPackages()) {
							Context	packageContext;
							
							packageContext = _c.genPackageClasses(genPackageClasses);
							generate(packageContext, loopElements, outputDir, ++newLoopIndex, depth + 1);
						}
						break;
					case Classes:
						//_c = c.loopElements(c.getPackageClasses().size());
						for (Class _class : c.getPackageClasses()) {
							Context	classContext;
							
							classContext = _c.class_(_class);
							generate(classContext, loopElements, outputDir, ++newLoopIndex, depth + 1);
						}
						break;
					case Methods:
						//_c = c.loopElements(c.getClass_().getDeclaredMethods().length);
						for (Method m : getMethodsForGeneration(c.getClass_())) {
						//for (Method m : c.getClass_().getMethods()) {
							if (!Modifier.isStatic(m.getModifiers()) && Modifier.isPublic(m.getModifiers())) {
								System.out.printf("Method\t%s\t%s\t%s\n", m.getName(), !JNIUtil.omitted(m), referenceFinder.allReferencesPresent(m));
							}
							if (!Modifier.isStatic(m.getModifiers()) && Modifier.isPublic(m.getModifiers()) && !JNIUtil.omitted(m) && referenceFinder.allReferencesPresent(m)) {
								Context	methodContext;
								
								methodContext = _c.method(m);
								generate(methodContext, loopElements, outputDir, ++newLoopIndex, depth + 1);
							}
						}
						break;
					case StaticMethods:
						//_c = c.loopElements(c.getClass_().getDeclaredMethods().length);
						for (Method m : getMethodsForGeneration(c.getClass_())) {
						//for (Method m : c.getClass_().getMethods()) {
							if (Modifier.isStatic(m.getModifiers()) && Modifier.isPublic(m.getModifiers()) && !JNIUtil.omitted(m) && referenceFinder.allReferencesPresent(m)) {
								Context	methodContext;
								
								methodContext = _c.method(m);
								generate(methodContext, loopElements, outputDir, ++newLoopIndex, depth + 1);
							}
						}
						break;
					case Constructors:
						for (Constructor constructor : c.getClass_().getConstructors()) {
							if (Modifier.isPublic(constructor.getModifiers()) && !JNIUtil.omitted(constructor) && referenceFinder.allReferencesPresent(constructor)) {
								Context	constructorContext;
								
								constructorContext = _c.constructor(constructor);
								generate(constructorContext, loopElements, outputDir, ++newLoopIndex, depth + 1);
							}
						}
						break;
					case NonEmptyConstructors:
						for (Constructor constructor : c.getClass_().getConstructors()) {
							if (Modifier.isPublic(constructor.getModifiers()) && !JNIUtil.omitted(constructor) && referenceFinder.allReferencesPresent(constructor)) {
								if (constructor.getParameterCount() > 0) {
									Context	constructorContext;
									
									constructorContext = _c.constructor(constructor);
									generate(constructorContext, loopElements, outputDir, ++newLoopIndex, depth + 1);
								}
							}
						}
						break;
					case Parameters:
						_c = c.loopElements(c.getActiveParameters().length);
						for (Parameter p : c.getActiveParameters()) {
							Context	parameterContext;
							
							parameterContext = _c.parameter(p);
							generate(parameterContext, loopElements, outputDir, ++newLoopIndex, depth + 1);
						}
						break;
					case StaticFields:
						for (Field f : getStaticFieldsForGeneration(c.getClass_())) {
							if (referenceFinder.referencePresent(f.getClass())) {
								Context	fieldContext;
								
								fieldContext = _c.field(f);
								generate(fieldContext, loopElements, outputDir, ++newLoopIndex, depth + 1);
							}
						}
						break;
					case Enums:
						for (Class ec : getEnumsForGeneration(c.getClass_())) {
							if (referenceFinder.referencePresent(ec)) {
								Context	enumContext;
								
								enumContext = _c.enum_(ec);
								generate(enumContext, loopElements, outputDir, ++newLoopIndex, depth + 1);
							}
						}
						break;
					case EnumValues:
						for (String v : getEnumValuesForGeneration(c.getEnum())) {
							Context	enumValueContext;
							
							enumValueContext = _c.enumValue(v);
							generate(enumValueContext, loopElements, outputDir, ++newLoopIndex, depth + 1);
						}
						break;
					case Interfaces:
						for (Class i_ : getInterfacesForGeneration(c.getClass_())) {
							if (referenceFinder.referencePresent(i_)) {
								Context	iContext;
								
								iContext = _c.interface_(i_);
								generate(iContext, loopElements, outputDir, ++newLoopIndex, depth + 1);
							}
						}
						break;
					case InheritedClasses:
						Set<Class>	inheritedClasses;
						Set<Class>	ic2;
						
						inheritedClasses = JNIUtil.getAllInheritedClasses(c.getClass_());
						ic2 = new HashSet<>();
						for (Class inheritedClass : inheritedClasses) {
							if (isBaseClass(inheritedClass) || referenceFinder.referencePresent(inheritedClass)) {
								ic2.add(inheritedClass);
							}
						}
						_c = c.loopElements(ic2.size());
						for (Class inheritedClass : ic2) {
							Context	icContext;
							
							icContext = _c.inheritedClass(inheritedClass);
							generate(icContext, loopElements, outputDir, ++newLoopIndex, depth + 1);
						}
						break;
					case ReferencedClasses:
						Set<Class>	_referencedClasses;
						Set<Class>	referencedClasses;
						Set<Class>	_inheritedClasses;
						Set<Class>	rc2;
						
						_inheritedClasses = JNIUtil.getAllInheritedClasses(c.getClass_());
						//System.out.printf("\ngetReferencedClasses\t%s\n", c.getClass_().getName());
						_referencedClasses = JNIUtil.getReferencedClasses(c.getClass_());
						referencedClasses = new HashSet<>();
						referencedClasses.addAll(_referencedClasses);
						referencedClasses.addAll(_inheritedClasses);
						//System.out.printf("\n%s\t%s\n\n", c.getClass_().getName(), CollectionUtil.toString(referencedClasses));
						rc2 = new HashSet<>();
						for (Class referencedClass : referencedClasses) {
							if (referenceFinder.referencePresent(referencedClass)) {
								rc2.add(referencedClass);
							} else {
								//System.out.printf("ReferencedClasses ignoring %s\n", referencedClass.getName());
							}
						}
						_c = c.loopElements(rc2.size());
						for (Class referencedClass : rc2) {
							Context	rcContext;
							
							//System.out.printf("ReferencedClasses adding %s\n", referencedClass.getName());
							rcContext = _c.referencedClass(referencedClass);
							generate(rcContext, loopElements, outputDir, ++newLoopIndex, depth + 1);
						}
						break;
					default: throw new RuntimeException("Panic");
					}
					i += loopElements.size() + 2;
				}
			} else if (e instanceof IfElement) {
				Quadruple<IfElement,List<ParseElement>,List<ParseElement>,Integer>	ifStatement;
				
				ifStatement = getIfElements(elements, i);
				generate(c, IfElement.evaluate(c, ifStatement.getTripleAt1()), outputDir, loopIndex, depth + 1);
				i = ifStatement.getV4();
			} else if (e instanceof SwitchElement) {
				Triple<SwitchElement,List<Pair<CaseElement,List<ParseElement>>>,Integer>	switchStatement;
				
				switchStatement = getSwitchElements(elements, i);
				generate(c, SwitchElement.evaluate(c, switchStatement.getPairAt1()), outputDir, loopIndex, depth + 1);
				i = switchStatement.getV3();
			} else {
				throw new RuntimeException("Unhandled element "+ e);
			}
		}
	}

	private Class[] getEnumsForGeneration(Class class_) {
		return JNIUtil.getInnerEnums(class_);
	}

	private String[] getEnumValuesForGeneration(Class class_) {
		if (!class_.isEnum()) {
			throw new RuntimeException(String.format("%s is not an Enum", class_.getName())); 
		} else {
			List<String>	values;
			
			values = new ArrayList<>();
			for (Object o : class_.getEnumConstants()) {
				values.add(o.toString());
			}
			return values.toArray(new String[0]);
		}
	}

	private Class[] getInterfacesForGeneration(Class class_) {
		List<Class>	values;
		
		values = new ArrayList<>();
		for (Class c : class_.getInterfaces()) {
			values.add(c);
		}
		return values.toArray(new Class[0]);
	}
	
	private boolean isValidLoopTarget(LoopElement curLoopElement, Target target) {
		if (curLoopElement == null) {
			return target == LoopElement.Target.Packages;
		} else {
			switch (curLoopElement.getTarget()) {
			case Packages:
				return target == LoopElement.Target.Classes;
			case Classes:
				return target == LoopElement.Target.Methods
						|| target == LoopElement.Target.NonEmptyConstructors
						|| target == LoopElement.Target.Constructors
						|| target == LoopElement.Target.StaticMethods
						|| target == LoopElement.Target.StaticFields
						|| target == LoopElement.Target.Enums
						|| target == LoopElement.Target.ReferencedClasses
						|| target == LoopElement.Target.Interfaces
						|| target == LoopElement.Target.InheritedClasses
						;
			case Methods:
				return target == LoopElement.Target.Parameters;
			case StaticMethods:
				return target == LoopElement.Target.Parameters;
			case Constructors:
				return  target == LoopElement.Target.Parameters;
			case NonEmptyConstructors:
				return  target == LoopElement.Target.Parameters;
			case Parameters:
				return false;
			case Enums:
				return target == LoopElement.Target.EnumValues;
			default:
				throw new RuntimeException("Panic");
			}
		}
	}
	
	/**
	 * Given a list of parse elements, and the index of a LoopElement in that list, 
	 * extract the ParseElements that are inside of the given loop.
	 * @param elements	list of parse elements
	 * @param le0Index	start index of the loop in the list
	 * @return the ParseElements that are inside of the given loop
	 */
	private List<ParseElement> getLoopElements(List<ParseElement> elements, int le0Index) {
		List<ParseElement>	loopElements;
		LoopElement			le0;
		
		le0 = (LoopElement)elements.get(le0Index);
		loopElements = new ArrayList<>();
		// simplistic, doesn't support nesting
		for (int i = le0Index + 1; i < elements.size(); i++) {
			ParseElement	e;
			
			e = elements.get(i);
			if (e instanceof LoopElement) {
				LoopElement	le1;
				
				le1 = (LoopElement)e;
				if (le1.getTarget().equals(le0.getTarget())) {
					return loopElements;
				} else {
					loopElements.add(e);
				}
			} else {
				loopElements.add(e);
			}
		}
		throw new RuntimeException(String.format("No loop end for %s", le0));
	}

	/**
	 * Given a list of parse elements, and the index of an IfElement in that list, 
	 * extract the ParseElements that are inside of the given If statement.
	 * @param elements	list of parse elements
	 * @param if0Index	start index of the loop in the list
	 * @return the ParseElements that are inside of the given if statement
	 */
	private Quadruple<IfElement,List<ParseElement>,List<ParseElement>,Integer> getIfElements(List<ParseElement> elements, int if0Index) {
		List<ParseElement>	thenElements;
		List<ParseElement>	elseElements;
		IfElement			ifElement;
		boolean				inElse;
		
		ifElement = (IfElement)elements.get(if0Index);
		thenElements = new ArrayList<>();
		elseElements = new ArrayList<>();
				
		inElse = false;
		// simplistic, doesn't support nesting
		for (int i = if0Index + 1; i < elements.size(); i++) {
			ParseElement	e;
			
			e = elements.get(i);
			if (e instanceof IfElement) {
				IfElement	ie;
				
				ie = (IfElement)e;
				switch (ie.getType()) {
				case If: throw new RuntimeException("Nested Ifs not supported");
				case Else: 
					if (!inElse) {
						inElse = true;
					} else {
						 throw new RuntimeException("Double Else detected");
					}
					break;
				case EndIf: return new Quadruple<>(ifElement, thenElements, elseElements, i + 1);
				default: throw new RuntimeException("Panic");
				}
			} else {
				if (!inElse) {
					thenElements.add(e);
				} else {
					elseElements.add(e);
				}
			}
		}
		throw new RuntimeException(String.format("No If end for %s", ifElement));
	}
	
	/**
	 * Given a list of parse elements, and the index of an SwitchElement in that list, 
	 * extract the ParseElements that are inside of the given Switch statement.
	 * @param elements	list of parse elements
	 * @param switchIndex	start index of the loop in the list
	 * @return the ParseElements that are inside of the given if statement
	 */
	private Triple<SwitchElement,List<Pair<CaseElement,List<ParseElement>>>,Integer> getSwitchElements(List<ParseElement> elements, int switchIndex) {
		List<Pair<CaseElement,List<ParseElement>>>	caseElements;
		SwitchElement		switchElement0;
		List<ParseElement>	curCaseElementList;
		CaseElement	curCaseElement;
		
		caseElements = new ArrayList<>();		
		switchElement0 = (SwitchElement)elements.get(switchIndex);
				
		if (debugSwitchElements) {
			System.out.printf("getSwitchElements()\n");
		}
		curCaseElement = null;
		curCaseElementList = null;		
		// simplistic, doesn't support nesting
		for (int i = switchIndex + 1; i < elements.size(); i++) {
			ParseElement	e;
			
			e = elements.get(i);
			if (e instanceof SwitchElement) {
				SwitchElement		switchElement1;
				
				switchElement1 = (SwitchElement)e;
				if (switchElement1.getType() == SwitchElement.Type.EndSwitch) {
					if (debugSwitchElements) {
						System.out.printf("out getSwitchElements()\n");
					}
					if (curCaseElement != null) {
						caseElements.add(new Pair<>(curCaseElement, curCaseElementList));
					}
					return new Triple<>(switchElement0, caseElements, i + 1);
				} else {
					throw new RuntimeException("Nested SwitchElements not supported");
				}
			}
			if (e instanceof CaseElement) {
				if (debugSwitchElements) {
					System.out.printf("New CaseElement %s\n", e);
				}
				if (curCaseElement != null) {
					caseElements.add(new Pair<>(curCaseElement, curCaseElementList));
				}
				curCaseElement = (CaseElement)e;
				curCaseElementList = new ArrayList<>();		
			} else {
				if (curCaseElementList == null) {
					// ignore until we get to a case
					if (debugSwitchElements) {
						System.out.printf("se ignoring %s\n", e);
					}
				} else {
					if (debugSwitchElements) {
						System.out.printf("se adding %s\n", e);
					}
					curCaseElementList.add(e);
				}
			}
		}
		throw new RuntimeException(String.format("No Switch end for %s", switchElement0));
	}
	
	private void generateLoopTargetException(LoopElement le) {
		throw new RuntimeException(String.format("Invalid target for %s", le));
	}

	private void generateLoopTargetException(LoopElement le, Target expectedTarget) {
		throw new RuntimeException(String.format("Invalid target for %s. Expected %s.", le, expectedTarget));
	}
	
	/**
	 * Currently this method is only for debugging
	 * @param templateFile
	 * @param outputDir
	 * @throws IOException
	 */
	public void generate(File templateFile, File outputDir) throws IOException {
		if (!outputDir.exists()) {
			throw new RuntimeException("outputDir does not exist: "+ outputDir);
		} else {
			List<ParseElement>	elements;
			
			elements = parseElements(templateFile);
			//displayElements(elements);
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

	/**
	 * Parse the template file into a list of ParseElements
	 * @param templateFile
	 * @return list of ParseElements
	 * @throws IOException
	 */
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
					if (debugParse) {
						System.out.printf("\t%d\t%s\t%s\n", elements.size(), s.substring(i, nextStart), "Text");
					}
					elements.add(new Text(s.substring(i, nextStart)));
				}
				i = nextStart + options.startDelimiter.length();
				state = ParseState.InsideExpression;
				break;
			case InsideExpression:
				Statement	statement;
				
				if (nextStart < nextEnd) {
					generateParseException(s, "Unexpected "+ options.startDelimiter, i);
				}
				if (nextEnd == length) {
					generateParseException(s, "Missing "+ options.endDelimiter, i);
				}
				statement = StatementParser.parse(s.substring(i, nextEnd));
				if (debugParse) {
					System.out.printf("\t%d\t%s\t%s\n", elements.size(), s.substring(i, nextEnd), statement);
				}
				elements.add(statement);
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
    		Variable.setTypeMappings(options.typeMappingFile == null ? ImmutableList.of() : TypeMapping.readTypeMappings(options.typeMappingFile));
    		wg = new WrapperGenerator(options);
    		wg.generate(GenPackageClasses.createForPackagesAndClasses(new File(options.codebase), options.inputPackagesAndClasses), new File(options.templateFile), new File (options.outputDir), options.addDependencies);
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
	}
}
