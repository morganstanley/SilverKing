package com.ms.silverking.cloud.dht.client.gen;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ms.silverking.collection.EnumUtil;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.text.StringUtil;

public class Variable implements Expression {
	private final String		name;
	private final VariableType	type;
	private final CaseType		caseType;
	
	private static Map<String,TypeMapping>	javaTypeMappings;
	private static Map<String,TypeMapping>	externalTypeMappings;
	private static Set<String>	mappedJavaTypes;
	
	private static final String	unalteredCasePrefix = "$";
	
	private static final int classMD5Length = 12;
	
	private enum VariableType {Class, Package, MethodName, LoopIndex, LoopElements,
								ClassMD5,
								ClassHasEmptyConstructor,
								MethodReturnType, MethodReturnTypeSimple, MethodReturnTypeWrapper, MethodReturnTypeRaw, MethodReturnTypePackage, 
								MethodSignature, ConstructorSignature, StaticFieldSignature,
								StaticFieldName, StaticFieldType, StaticFieldTypeSimple, StaticFieldTypeRaw,
								Enum, EnumValue,
								Interface, InterfacePackage,
								ReferencedClassSimple,
								InheritedClass, InheritedClassPackage,
								ImplementsInterfaces, NonVirtual,
								ParameterName, ParameterNameWrapped, ParameterTypePackage, ParameterType, ParameterTypeSimple, ParameterIsPrimitiveOrEnum, ParameterIsPrimitive, ParameterIsObject, ParameterIsUserObject, ReturnTypeIsPrimitive, StaticFieldTypeIsPrimitive,
								SuperClass, SuperClassPackage, EmptyString, JNICallType};
	private enum CaseType {alllowercase, ALLUPPERCASE, camelCase, CamelCase, unalteredCase};
	
	public Variable(String name) {
		if (name.startsWith(unalteredCasePrefix)) {
			name = name.substring(unalteredCasePrefix.length());
			caseType = CaseType.unalteredCase;
		} else {
			caseType = _getCaseType(name);
		}
		this.name = name;
		this.type = EnumUtil.valueOfIgnoreCase(VariableType.class, name);
		if (type == null) {
			throw new RuntimeException("Unknown variable: "+ name);
		}
	}
	
	public VariableType getType() {
		return type;
	}
	
	public static void setTypeMappings(List<TypeMapping> typeMappings) {
		javaTypeMappings = new HashMap<>();
		externalTypeMappings = new HashMap<>();
		mappedJavaTypes = new HashSet<>();
		for (TypeMapping typeMapping : typeMappings) {
			mappedJavaTypes.add(typeMapping.getJavaType());
			javaTypeMappings.put(typeMapping.getJavaType(), typeMapping);
			externalTypeMappings.put(typeMapping.getExternalType(), typeMapping);
		}
	}
	
	public static boolean isMappedJavaType(String s) {
		return mappedJavaTypes.contains(s);
	}
	
	private static CaseType _getCaseType(String name) {
		if (name.length() == 0) {
			throw new RuntimeException("panic");
		} else {
			boolean	initialUpper;
			boolean	mixed;
			
			initialUpper = Character.isUpperCase(name.charAt(0));
			mixed = false;
			for (int i = 1; i < name.length(); i++) {
				if (Character.isUpperCase(name.charAt(i)) != initialUpper) {
					mixed = true;
					break;
				}
			}
			if (initialUpper) {
				if (mixed) {
					return CaseType.CamelCase;
				} else {
					return CaseType.ALLUPPERCASE;
				}
			} else {
				if (mixed) {
					return CaseType.camelCase;
				} else {
					return CaseType.alllowercase;
				}
			}
		}
	}
	
	private CaseType getCaseType() {
		return caseType;
	}
	
	private String matchPattern(String value) {
		if (value == null) {
			return value;
		} else {
			switch (getCaseType()) {
			case alllowercase: return value.toLowerCase(); 
			case ALLUPPERCASE: return value.toUpperCase();
			case camelCase: return tocamelCase(value);
			case CamelCase: return toCamelCase(value);
			case unalteredCase: return value;
			default: throw new RuntimeException("panic");
			}
		}
	}
	
	private String tocamelCase(String value) {
		if (value.length() == 0) {
			return value;
		} else {
			StringBuffer	sb;
			
			sb = new StringBuffer();
			sb.append(Character.toLowerCase(value.charAt(0)));
			sb.append(value.substring(1));
			return sb.toString();
		}
	}
	
	private String toCamelCase(String value) {
		if (value.length() == 0) {
			return value;
		} else {
			StringBuffer	sb;
			
			sb = new StringBuffer();
			sb.append(Character.toUpperCase(value.charAt(0)));
			sb.append(value.substring(1));
			return sb.toString();
		}
	}

	@Override
	public Pair<Context,String> evaluate(Context c) {
		return new Pair<>(c, matchPattern(_evaluate(c)));
	}
	
	private String _evaluate(Context c) {
		switch(type) {
		case Class: return c.getClass_().getSimpleName();
		case ClassHasEmptyConstructor: return hasEmptyConstructor(c.getClass_()) ? "true" : "false";
		case ClassMD5: return StringUtil.trimLength(StringUtil.md5(c.getClass_().getSimpleName()).replace(':', '_'), classMD5Length);
		case Package: return c.getPackage_().getName();
		case MethodName: return c.getMethod().getName();
		case LoopIndex: return Integer.toString(c.getLoopIndex());
		case LoopElements: return Integer.toString(c.getLoopElements());
		case MethodReturnTypeRaw: return getReturnTypeRaw(c.getMethod());
		case MethodReturnType: return getReturnType(c.getMethod(), false, null);
		case MethodReturnTypeSimple: return getReturnType(c.getMethod(), true, c.getEnclosingTypeSeparator());
		case MethodReturnTypeWrapper: return getReturnTypeWrapper(c.getMethod());
		case MethodReturnTypePackage: return getReturnTypePackage(c.getMethod());
		case MethodSignature: return JNIUtil.getJNISignature(c.getMethod());
		case ConstructorSignature: return JNIUtil.getJNISignature(c.getConstructor());
		case ParameterName: return c.getParameter().getName();
		case ParameterNameWrapped: return getParameterNameWrapped(c.getParameter());
		case ParameterTypePackage: return getParameterTypePackage(c.getParameter());
		case ParameterType: return getParameterType(c.getParameter(), false, null);
		case ParameterTypeSimple: return getParameterType(c.getParameter(), true, c.getEnclosingTypeSeparator());
		case ParameterIsPrimitiveOrEnum: return c.getParameter() != null && (c.getParameter().getType().isPrimitive() || c.getParameter().getType().isEnum()) ? "true" : "false"; 
		case ParameterIsPrimitive: return c.getParameter() != null && (c.getParameter().getType().isPrimitive()) ? "true" : "false"; 
		case ParameterIsObject: return isObject(c.getParameter()) ? "true" : "false"; 
		case ParameterIsUserObject: return isUserObject(c.getParameter()) ? "true" : "false"; 
		case ReturnTypeIsPrimitive: return c.getMethod() != null && c.getMethod().getReturnType() != null && (c.getMethod().getReturnType().isPrimitive()) ? "true" : "false"; 
		case StaticFieldTypeIsPrimitive: return c.getField().getType() != null && (c.getField().getType().isPrimitive()) ? "true" : "false";
		case StaticFieldSignature: return JNIUtil.getJNISignature(c.getField());
		case StaticFieldName: return c.getField().getName();
		case StaticFieldType: return getFieldType(c.getField(), false, null);
		case StaticFieldTypeSimple: return getFieldType(c.getField(), true, c.getEnclosingTypeSeparator());
		case StaticFieldTypeRaw: return getFieldTypeRaw(c.getField());
		case Enum: return c.getEnum().getSimpleName();
		case EnumValue: return c.getEnumValue();
		case Interface: return c.getInterface().getSimpleName();
		case InterfacePackage: return c.getInterface().getPackage().getName();
		case ReferencedClassSimple: return c.getReferencedClass().getSimpleName();
		case InheritedClass: return c.getInheritedClass().getSimpleName();
		case InheritedClassPackage: return c.getInheritedClass().getPackage().getName();
		case SuperClass: return getSuperclassName(c.getClass_());
		case SuperClassPackage: return getSuperclassPackage(c.getClass_());
		case ImplementsInterfaces: return Boolean.toString(c.getClass_().getInterfaces().length > 0);
		case NonVirtual: return Boolean.toString(JNIUtil.nonVirtual(c.getClass_()));
		case JNICallType: return JNIUtil.getCallType(c.getMethod().getReturnType());
		case EmptyString: return "";
		default: throw new RuntimeException("Unknown variable: "+ name);
		}
	}
	
	private boolean hasEmptyConstructor(Class class_) {
		for (Constructor c : class_.getConstructors()) {
			if (c.getParameterCount() == 0) {
				return true;
			}
		}
		return false;
	}

	private boolean isObject(Parameter p) {
		return p != null && (!p.getType().isPrimitive()) && (!p.getType().isArray())
				&& (!p.getType().isEnum()) && (!p.getType().getPackage().getName().startsWith("java"));
	}

	private boolean isUserObject(Parameter p) {
		return isObject(p) && !p.getType().equals(java.lang.String.class);
	}
	
	private String getSuperclassPackage(Class c) {
		return getSuperclass(c).getPackage().getName();
	}
	
	private String getSuperclassName(Class c) {
		return getSuperclass(c).getSimpleName();
	}
	
	private Class getSuperclass(Class c) {
		System.out.printf("superclass %s %s\n", c.getName(), c.getSuperclass());
		return c.getSuperclass() != null ? c.getSuperclass() : Object.class;		
	}

	private static String getPackageName(Class c) {
		if (c.isPrimitive()) {
			return "";
		} else {
			return c.getPackage().getName();
		}
	}
	
	private static String getTypePackage(Class c) {
		if (c.isArray()) {
			return getPackageName(c.getComponentType());
		} else {
			return getPackageName(c);
		}
	}
	
	private String getReturnTypePackage(Method m) {
		//return getTypePackage(m.getReturnType());
		
		String	name;
		TypeMapping	javaTypeMapping;
		
		name = m.getReturnType().getTypeName();
		javaTypeMapping = javaTypeMappings.get(name);
		//System.out.printf("\t###\t%s\t%s\n", name, javaTypeMapping);
		if (javaTypeMapping == null) {
			return getTypePackage(m.getReturnType());
		} else {
			return "";
		}
	}

	private static String getTypeNameForMapping(Class c) {
		String	n;
		
		n = c.getName();
		if (n.startsWith("class ")) {
			n = n.substring("class ".length());
		}
		return n;
	}
	
	private String getTypeNameForMapping(Type t) {
		String	n;
		
		n = t.getTypeName();
		if (n.startsWith("class ")) {
			n = n.substring("class ".length());
		}
		return n;
	}
	
	/*
	public static String getReturnTypeWrapper(Method m, String s) {
		String	name;
		TypeMapping	javaTypeMapping;
		
		name = m.getReturnType().toString();
		javaTypeMapping = javaTypeMappings.get(name);
		if (javaTypeMapping != null) {
			return javaTypeMapping.getSKToJavaGenerator().generate(s);
		} else {
			return s;
		}
	}
	*/
	public static String getReturnTypeWrapper(Method m) {
		String	name;
		TypeMapping	typeMapping;
		
		name = getTypeNameForMapping(m.getReturnType());
		System.out.printf("getReturnTypeWrapper %s %s\n", m.getName(), name);
		typeMapping = javaTypeMappings.get(name);
		if (typeMapping != null) {
			String	wrapper;
			
			wrapper = typeMapping.getJavaToSKGenerator().getReturnWrapper();
			if (wrapper != null) {
				return wrapper;
			} else {
				return "";
			}
		} else {
			return "";
		}
	}

	private String getParameterNameWrapped(Parameter p) {
		String	name;
		TypeMapping	javaTypeMapping;
		
		name = getTypeNameForMapping(p.getParameterizedType());
		javaTypeMapping = javaTypeMappings.get(name);
		if (javaTypeMapping != null) {
			return javaTypeMapping.getSKToJavaGenerator().generate(p.getName());
		} else {
			return p.getName();
		}
	}

	private static String getTypedClassName(Class c) {
		String	g;
		int		i;
		
		g = c.toGenericString();
		i = g.indexOf(c.getName());
		return g.substring(i);
	}

	private static String getParameterTypePackage(Parameter p) {
		String	name;
		TypeMapping	javaTypeMapping;
		
		name = p.getParameterizedType().getTypeName();
		//name = p.getParameterizedType().toString();
		javaTypeMapping = javaTypeMappings.get(name);
		//System.out.printf("\t###\t%s\t%s\n", name, javaTypeMapping);
		if (javaTypeMapping == null) {
			return getTypePackage(p.getType());
		} else {
			return "";
		}
	}

	private static String getParameterType(Parameter p, boolean simple, String enclosingTypeSeparator) {
		String	name;
		TypeMapping	javaTypeMapping;
		
		//name = p.getParameterizedType().toString();
		name = p.getParameterizedType().getTypeName();
		javaTypeMapping = javaTypeMappings.get(name);
		//System.out.printf("\t***%s\t%s\n", p.getName(), javaTypeMapping == null ? "null" : javaTypeMapping.getExternalType());
		if (javaTypeMapping == null) {
			System.out.printf("%s\n", name);
		}
		if (javaTypeMapping == null) {
			if (!simple) {
				return name;
			} else {
				return getSimpleTypeName(p.getType(), enclosingTypeSeparator);
				//return p.getType().getSimpleName();
			}
		} else {
			return javaTypeMapping.getExternalType();
		}
	}

	private static String getFieldType(Field f, boolean simple, String enclosingTypeSeparator) {
		String	name;
		TypeMapping	javaTypeMapping;
		
		name = f.getType().toString();
		javaTypeMapping = javaTypeMappings.get(name);
		if (javaTypeMapping == null) {
			System.out.printf("%s\n", name);
		}
		if (javaTypeMapping == null) {
			if (!simple) {
				return name;
			} else {
				return getSimpleTypeName(f.getType(), enclosingTypeSeparator);
				//return f.getType().getSimpleName();
			}
		} else {
			return javaTypeMapping.getExternalType();
		}
	}
	
	private static String getFieldTypeRaw(Field f) {
		return f.getType().getName();
	}
	
	private static String getSimpleTypeName(Class c, String enclosingTypeSeparator) {
		Class	e;
		
		e = c.getEnclosingClass();
		if (e == null) {
			return c.getSimpleName();
		} else {
			return e.getSimpleName() + enclosingTypeSeparator + c.getSimpleName();
		}
	}
	
	private static String getReturnType(Method m, boolean simple, String enclosingTypeSeparator) {
		String	name;
		TypeMapping	javaTypeMapping;
		
		name = m.getReturnType().getTypeName();
		//name = m.toGenericString();
		//name = m.getGenericReturnType().getTypeName();
		//System.out.printf("### %s\n", m.getGenericReturnType().getTypeName());
		javaTypeMapping = javaTypeMappings.get(name);
		if (javaTypeMapping == null) {
			System.out.printf("\t%s\n", name);
		}
		if (javaTypeMapping == null) {
			if (!simple) {
				return name;
			} else {
				return getSimpleTypeName(m.getReturnType(), enclosingTypeSeparator);
				//return m.getReturnType().getSimpleName();
			}
		} else {
			return javaTypeMapping.getExternalType();
		}
	}
	
	private static String getReturnTypeRaw(Method m) {
		//System.out.printf("\t###%s\t%s\t%s\n", m.getName(), m.getReturnType().getName(), m.getGenericReturnType().getTypeName());
		return m.getReturnType().getName();
		//return m.getGenericReturnType().getTypeName();
	}
	
	public static boolean isVariable(String name) {
		try {
			if (name.startsWith(unalteredCasePrefix)) {
				name = name.substring(unalteredCasePrefix.length());
			}
			return EnumUtil.valueOfIgnoreCase(VariableType.class, name) != null;
		} catch (IllegalArgumentException iae) {
			return false;
		}
	}
}
