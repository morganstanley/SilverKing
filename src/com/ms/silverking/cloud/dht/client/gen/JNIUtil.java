package com.ms.silverking.cloud.dht.client.gen;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.GetOptions;
import com.ms.silverking.collection.CollectionUtil;

public class JNIUtil {
	private static final boolean	debugReferencedClasses = false;
	
	private static final Set<String>	enumMethodOmissions;
	
	static {
		enumMethodOmissions = ImmutableSet.of("values", "valueOf");
	}
	
	public static String getJNISignature(Constructor c) {
		StringBuffer	sb;
		
		sb = new StringBuffer();
		sb.append('(');
		for (Parameter p : c.getParameters()) {
			sb.append(typeString(p.getType()));
		}
		sb.append(")V");
		return sb.toString();
	}
	
	public static String getJNISignature(Method m) {
		StringBuffer	sb;
		
		sb = new StringBuffer();
		sb.append('(');
		for (Parameter p : m.getParameters()) {
			sb.append(typeString(p.getType()));
		}
		sb.append(')');
		sb.append(typeString(m.getReturnType()));
		return sb.toString();
	}
	
	public static String getJNISignature(Field f) {
		StringBuffer	sb;
		
		sb = new StringBuffer();
		sb.append(typeString(f.getType()));
		return sb.toString();
	}

	private static String typeString(Class<?> type) {
		StringBuffer	sb;
		
		sb = new StringBuffer();
		if (type.isPrimitive()) {
			if (type.isArray()) {
				sb.append("[");
			}
			sb.append(primitiveTypeString(type));
		} else {
			if (type.isArray()) {
				sb.append(type.getName().replace('.', '/'));
			} else {
				sb.append('L');
				sb.append(type.getName().replace('.', '/'));
				sb.append(';');
			}
		}
		return sb.toString();
	}
	
	public long test(int n, String s, int[] arr) {
		return 0;
	}
	
	private static String primitiveTypeString(Class<?> type) {
		if (type == boolean.class) {
			return "Z";
		} else if (type == byte.class) {
			return "B";
		} else if (type == char.class) {
			return "C";
		} else if (type == short.class) {
			return "S";
		} else if (type == int.class) {
			return "I";
		} else if (type == long.class) {
			return "J";
		} else if (type == float.class) {
			return "F";
		} else if (type == double.class) {
			return "D";
		} else if (type == void.class) {
			return "";
		} else {
			throw new RuntimeException("Unknown primitive type: "+ type);
		}
	}

	public static String getCallType(Class<?> type) {
		if (type.isPrimitive()) {
			return type.getName();
		} else {
			if (type == boolean.class) {
				return "Boolean";
			} else if (type == byte.class) {
				return "Byte";
			} else if (type == char.class) {
				return "Char";
			} else if (type == short.class) {
				return "Short";
			} else if (type == int.class) {
				return "Int";
			} else if (type == long.class) {
				return "Long";
			} else if (type == float.class) {
				return "Float";
			} else if (type == double.class) {
				return "Double";
			} else if (type == void.class) {
				return "Void";
			} else {
				return "Object";
			}
		}
	}
	
	public static Class[] getInnerEnums(Class class_) {
		List<Class>	enums;
		
		enums = new ArrayList<>();
		for (Class c : class_.getDeclaredClasses()) {
			if (c.isEnum()) {
				enums.add(c);
			}
		}
		return enums.toArray(new Class[0]);
	}
	
	public static boolean isReferencedClass(Class c) {
		return !c.isPrimitive() && !omitReferencedClass(c);
	}
	
	public static <E extends Executable> Set<Class> getReferencedClasses(E e) {
		Set<Class>	s;
		
		s = new HashSet<>();
		if (!omitted(e)) {
			if (debugReferencedClasses) {
				System.out.printf("\tgetReferencedClassesE %s\n", e.getName());
			}
			if (e instanceof Method) {
				if (isReferencedClass(((Method)e).getReturnType())) {
					if (debugReferencedClasses) {
						System.out.printf("\tAdding return type %s\n", ((Method)e).getReturnType());
					}
					s.add(((Method)e).getReturnType());
				} else {
					if (debugReferencedClasses) {
						System.out.printf("Not referenced class1 %s %s\n", e.getName(), ((Method)e).getReturnType());
					}
				}
			}
			for (Parameter p : e.getParameters()) {
				if (omitted(p.getType())) {
					
				}
				if (isReferencedClass(p.getType())) {
					if (debugReferencedClasses) {
						System.out.printf("\tAdding parameter %s\n", p.getType());
					}
					s.add(p.getType());
				} else {
					if (debugReferencedClasses) {
						System.out.printf("Not referenced class2 %s %s\n", e.getName(), p.getType());
					}
				}
			}
		} else {
			if (debugReferencedClasses) {
				System.out.printf("Omitted %s\n", e.getName());
			}
		}
		return s;
	}
	
	public static Set<Class> getAllInheritedClasses(Class c) {
		Set<Class>	s;
		Set<Class>	cs;
		
		cs = new HashSet<>();
		cs.addAll(extractReferencedClasses(c.getInterfaces()));
		if (c.getSuperclass() != null) { // note that we do not filer superclasses on referenced classes so that we can populate Object,Enum,etc.
			cs.add(c.getSuperclass());
		}
		// recurse 
		s = new HashSet<>();
		s.addAll(cs);
		for (Class c_ : cs) {
			s.addAll(getAllInheritedClasses(c_));
		}
		s.add(Object.class);
		return s;
	}
	
	private static Set<Class> extractReferencedClasses(Class[] ca) {
		Set<Class>	cs;
		
		cs = new HashSet<>();
		for (Class c : ca) {
			if (isReferencedClass(c)) {
				cs.add(c);
			}
		}
		return cs;
	}
	
	public static <E extends Executable> Set<Class> getReferencedClasses(E[] ea) {
		Set<Class>	s;
		
		s = new HashSet<>();
		for (Executable e : ea) {
			s.addAll(getReferencedClasses(e));
		}
		return s;
	}

	public static Set<Class> getReferencedClasses(Class c) {
		Set<Class>	s;
		
		if (debugReferencedClasses) {
			System.out.printf("\n\ngetReferencedClasses a %s\n", c.getName());
		}
		s = new HashSet<>();
		if (c.isEnum()) {
			if (debugReferencedClasses) {
				System.out.printf("%s\n", CollectionUtil.toString(getReferencedClasses(c.getDeclaredMethods())));
			}
			for (Method m : c.getDeclaredMethods()) {
				if (!enumMethodOmissions.contains(m.getName())) {
					if (debugReferencedClasses) {
						System.out.printf("\t%s\n", m.getName());
					}
					s.addAll(getReferencedClasses(m));
				}
			}
			//s.addAll(getReferencedClasses(c.getDeclaredMethods()));
		} else {
			s.addAll(getReferencedClasses(c.getDeclaredMethods()));
		}
		s.addAll(getReferencedClasses(c.getConstructors()));
		s.addAll(extractReferencedClasses(c.getInterfaces()));
		s.removeAll(ImmutableSet.copyOf(getInnerEnums(c)));
		s.remove(c);
		return s;
	}
	
	private static Set<Class> filterEnums(Set<Class> s0) {
		Set<Class>	s1;
		
		s1 = new HashSet<>();
		for (Class c : s0) {
			if (!c.isEnum()) {
				s1.add(c);
			}
		}
		return s1;
	}
	
	public static boolean omitted(Executable e) {
		if (omitted(e.getDeclaringClass())) {
			return true;
		}
		//System.out.printf("e omitted %s %d\n", e.getName(), e.getAnnotations().length);
		for (Annotation a : e.getAnnotations()) {
			//System.out.printf("omitted a ##\t%s\t%s\t%s\n", e.getName(), a, a instanceof OmitGeneration);
			if (a instanceof OmitGeneration) {
				return true;
			}
		}
		return false;
	}
	
	public static boolean nonVirtual(Class c) {
		//System.out.printf("c omitted %s %d\n", c.getName(), c.getAnnotations().length);
		for (Annotation a : c.getAnnotations()) {
			//System.out.printf("omitted a ##\t%s\t%s\t%s\n", c.getName(), a, a instanceof OmitGeneration);
			if (a instanceof NonVirtual) {
				return true;
			}
		}
		return false;
	}
	
	public static boolean omitted(Class c) {
		//System.out.printf("c omitted %s %d\n", c.getName(), c.getAnnotations().length);
		for (Annotation a : c.getAnnotations()) {
			//System.out.printf("omitted a ##\t%s\t%s\t%s\n", c.getName(), a, a instanceof OmitGeneration);
			if (a instanceof OmitGeneration) {
				return true;
			}
		}
		return false;
	}
	
	private static boolean omitReferencedClass(Class c) {
		return omitted(c);
	}
	
	public static void main(String[] args) {
		//System.out.println(CollectionUtil.toString(getReferencedClasses(GetOptions.class)));
		for (Executable e : GetOptions.class.getDeclaredMethods()) {
			System.out.println(e);
		}
		
		System.out.println();
		System.out.println();
		
		for (Executable e : GetOptions.class.getMethods()) {
			System.out.println(e);
		}
	}
}
