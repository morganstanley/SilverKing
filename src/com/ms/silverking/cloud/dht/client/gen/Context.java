package com.ms.silverking.cloud.dht.client.gen;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.List;

/**
 * Current parsing context
 */
public class Context {
	private final List<GenPackageClasses>	packages;
	private final GenPackageClasses	genPackageClasses;
	private final File			outputDir;
	private final Class			_class;
	private final Constructor	constructor;
	private final Method		method;
	private final Parameter		parameter;
	private final Field			field;
	private final Class			_enum;
	private final String		enumValue;
	private final Class			interface_;
	private final String		outputFileName;
	private final LoopElement	loopElement;
	private final int			loopElements;
	private final int			loopIndex;
	private final Class			referencedClass;
	private final Class			inheritedClass;
	private final String		enclosingTypeSeparator;
	
	public Context(List<GenPackageClasses> packages, GenPackageClasses genPackageClasses, File outputDir, String outputFileName, Class _class, Constructor constructor, Method _method, Parameter parameter, Field field, Class _enum, String enumValue, Class interface_, LoopElement loopElement, int loopElements, int elementIndex, Class referencedClass, Class inheritedClass, String enclosingTypeSeparator) {
		if (_method != null && constructor != null) {
			throw new RuntimeException("_method != null && constructor != null");
		}
		this.packages = packages;
		this.genPackageClasses = genPackageClasses;
		this.outputDir = outputDir;
		this._class = _class;
		this.constructor = constructor;
		this.method = _method;
		this.parameter = parameter;
		this.field = field;
		this._enum = _enum;
		this.enumValue = enumValue;
		this.interface_ = interface_;
		this.outputFileName = outputFileName;
		this.loopElement = loopElement;
		this.loopElements = loopElements;
		this.loopIndex = elementIndex;
		this.referencedClass = referencedClass;
		this.inheritedClass = inheritedClass;
		this.enclosingTypeSeparator = enclosingTypeSeparator;
	}
	
	public Context(List<GenPackageClasses> packages, File outputDir) {
		this(packages, null, outputDir, null, null, null, null, null, null, null, null, null, null, -1, -1, null, null, null);
	}
	
	public List<GenPackageClasses> getPackages() {
		return packages;
	}
		
	public GenPackageClasses getGenPackageClasses() {
		return genPackageClasses;
	}
	
	public Package getPackage_() {
		return genPackageClasses.getPackage();
	}
	
	public List<Class> getPackageClasses() {
		return genPackageClasses.getClasses();
	}
	
	public File getOutputDir() {
		return outputDir;
	}
	
	public String getOutputFileName() {
		return outputFileName;
	}

	public Class getClass_() {
		return _class;
	}

	public Constructor getConstructor() {
		return constructor;
	}
	
	public Method getMethod() {
		return method;
	}
	
	public Parameter getParameter() {
		return parameter;
	}
	
	public Field getField() {
		return field;
	}
	
	public Class getEnum() {
		return _enum;
	}
	
	public String getEnumValue() {
		return enumValue;
	}
	
	public Class getInterface() {
		return interface_;
	}
	
	public LoopElement getLoopElement() {
		return loopElement;
	}
	
	public int getLoopElements() {
		return loopElements;
	}
	
	public boolean isLastLoopElement() {
		return loopIndex == loopElements;
	}
	
	public int getLoopIndex() {
		return loopIndex;
	}
	
	public Class getReferencedClass() {
		return referencedClass;
	}
	
	public Class getInheritedClass() {
		return inheritedClass;
	}
	
	public String getEnclosingTypeSeparator() {
		return enclosingTypeSeparator;
	}

	/////////
	
	public Context genPackageClasses(GenPackageClasses genPackageClasses) {
		return new Context(packages, genPackageClasses, outputDir, outputFileName, _class, constructor, method, parameter, field, _enum, enumValue, interface_, loopElement, loopElements, loopIndex, referencedClass, inheritedClass, enclosingTypeSeparator);
	}
	
	public Context outputFileName(String outputFileName) {
		return new Context(packages, genPackageClasses, outputDir, outputFileName, _class, constructor, method, parameter, field, _enum, enumValue, interface_, loopElement, loopElements, loopIndex, referencedClass, inheritedClass, enclosingTypeSeparator);
	}
	
	public Context class_(Class _class) {
		return new Context(packages, genPackageClasses, outputDir, outputFileName, _class, constructor, method, parameter, field, _enum, enumValue, interface_, loopElement, loopElements, loopIndex, referencedClass, inheritedClass, enclosingTypeSeparator);
	}
	
	public Context constructor(Constructor constructor) {
		return new Context(packages, genPackageClasses, outputDir, outputFileName, _class, constructor, method, parameter, field, _enum, enumValue, interface_, loopElement, loopElements, loopIndex, referencedClass, inheritedClass, enclosingTypeSeparator);
	}
	
	public Context method(Method method) {
		return new Context(packages, genPackageClasses, outputDir, outputFileName, _class, constructor, method, parameter, field, _enum, enumValue, interface_, loopElement, loopElements, loopIndex, referencedClass, inheritedClass, enclosingTypeSeparator);
	}

	public Context parameter(Parameter parameter) {
		return new Context(packages, genPackageClasses, outputDir, outputFileName, _class, constructor, method, parameter, field, _enum, enumValue, interface_, loopElement, loopElements, loopIndex, referencedClass, inheritedClass, enclosingTypeSeparator);
	}

	public Context field(Field field) {
		return new Context(packages, genPackageClasses, outputDir, outputFileName, _class, constructor, method, parameter, field, _enum, enumValue, interface_, loopElement, loopElements, loopIndex, referencedClass, inheritedClass, enclosingTypeSeparator);
	}
	
	public Context enum_(Class _enum) {
		return new Context(packages, genPackageClasses, outputDir, outputFileName, _class, constructor, method, parameter, field, _enum, enumValue, interface_, loopElement, loopElements, loopIndex, referencedClass, inheritedClass, enclosingTypeSeparator);
	}
	
	public Context enumValue(String enumValue) {
		return new Context(packages, genPackageClasses, outputDir, outputFileName, _class, constructor, method, parameter, field, _enum, enumValue, interface_, loopElement, loopElements, loopIndex, referencedClass, inheritedClass, enclosingTypeSeparator);
	}
	
	public Context interface_(Class interface_) {
		return new Context(packages, genPackageClasses, outputDir, outputFileName, _class, constructor, method, parameter, field, _enum, enumValue, interface_, loopElement, loopElements, loopIndex, referencedClass, inheritedClass, enclosingTypeSeparator);
	}
	
	public Context loopElement(LoopElement loopElement) {
		return new Context(packages, genPackageClasses, outputDir, outputFileName, _class, constructor, method, parameter, field, _enum, enumValue, interface_, loopElement, loopElements, loopIndex, referencedClass, inheritedClass, enclosingTypeSeparator);
	}
	
	public Context loopElements(int loopElements) {
		return new Context(packages, genPackageClasses, outputDir, outputFileName, _class, constructor, method, parameter, field, _enum, enumValue, interface_, loopElement, loopElements, loopIndex, referencedClass, inheritedClass, enclosingTypeSeparator);
	}
	
	public Context loopIndex(int loopIndex) {
		return new Context(packages, genPackageClasses, outputDir, outputFileName, _class, constructor, method, parameter, field, _enum, enumValue, interface_, loopElement, loopElements, loopIndex, referencedClass, inheritedClass, enclosingTypeSeparator);
	}

	public Context referencedClass(Class referencedClass) {
		return new Context(packages, genPackageClasses, outputDir, outputFileName, _class, constructor, method, parameter, field, _enum, enumValue, interface_, loopElement, loopElements, loopIndex, referencedClass, inheritedClass, enclosingTypeSeparator);
	}
	
	public Context inheritedClass(Class inheritedClass) {
		return new Context(packages, genPackageClasses, outputDir, outputFileName, _class, constructor, method, parameter, field, _enum, enumValue, interface_, loopElement, loopElements, loopIndex, referencedClass, inheritedClass, enclosingTypeSeparator);
	}
	
	public Context enclosingTypeSeparator(String enclosingTypeSeparator) {
		return new Context(packages, genPackageClasses, outputDir, outputFileName, _class, constructor, method, parameter, field, _enum, enumValue, interface_, loopElement, loopElements, loopIndex, referencedClass, inheritedClass, enclosingTypeSeparator);
	}
	
	public Parameter[] getActiveParameters() {
		if (method != null) {
			return method.getParameters();
		} else if (constructor != null) {
			return constructor.getParameters();
		} else {
			return null;
		}
	}
}
