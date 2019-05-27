package com.ms.silverking.cloud.dht.client.gen;

import java.io.File;
import java.lang.reflect.Executable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.collection.CollectionUtil;

public class ClassReferenceFinder {
	private final Map<Package,Set<Class>>	allowedClasses;
	private final Set<Class>	references; 
	private final Set<Class>	omissions;
	
	private static final boolean	debugReferencedClasses = true;
	
	public ClassReferenceFinder(List<GenPackageClasses> genPackageClasses) {
		allowedClasses = gpcListToMap(genPackageClasses);
		references = new HashSet<>();
		omissions = new HashSet<>();
	}
	
	private static Map<Package,Set<Class>> gpcListToMap(List<GenPackageClasses> genPackageClasses) {
		Map<Package,Set<Class>>	allowedClasses;
		
		allowedClasses = new HashMap<>();
		for (GenPackageClasses gpc : genPackageClasses) {
			Set<Class>	cs;
			
			cs = new HashSet<>();
			for (Class c : gpc.getClasses()) {
				cs.add(c);
			}
			allowedClasses.put(gpc.getPackage(), cs);
		}
		return allowedClasses;
	}
	
	private boolean isInAllowedClasses(Class c) {
		Set<Class>	cs;
		
		cs = allowedClasses.get(c.getPackage());
		if (cs == null) {
			return false;
		} else {
			if (cs.isEmpty()) {
				return true;
			} else {
				return cs.contains(c);
			}
		}
	}
	
	
	public Set<Class> getReferences() {
		return references;
	}
	
	private boolean isAllowed(Class c) {
		if (isInAllowedClasses(c)) {
			return !omissions.contains(c);
		} else {
			return false;
		}
	}

	private void addIfAllowed(Set<Class> s, Class c) {
		if (isAllowed(c)) {
			s.add(c);
		}
	}
	
	public void addReferencedClasses() {
		System.out.printf("\nAdding all referenced classes\n");
		for (Set<Class> cs : allowedClasses.values()) {
			for (Class c : cs) {
				addReferencedClasses(c);
			}
		}
	}
	
	/**
	 * Add all referenced classes including this class
	 * @param c
	 * @return
	 */
	public void addReferencedClasses(Class c) {
		System.out.printf("Adding referenced classes %s\n", c.getName());
		if (isAllowed(c)) {
			if (!references.contains(c)) {
				Set<Class>	s;
				
				references.add(c);
				s = new HashSet<>();
				// We add all publically visible references (ignore private)
				// Constructors
				s.addAll(getImmediateReferences(c.getConstructors()));
				// Methods
				s.addAll(getImmediateReferences(c.getMethods()));
				// Static fields
				s.addAll(getImmediateReferencesFromStaticFields(c.getFields()));
	
				// Remove inner enums from references since they are handled by the enclosing class
				s.removeAll(ImmutableSet.copyOf(JNIUtil.getInnerEnums(c)));
				
				for (Class r : s) {
					addReferencedClasses(r);
				}
			}
		}
	}
		
	private Collection<? extends Class> getImmediateReferencesFromStaticFields(Field[] fields) {
		Set<Class>	s;
		
		s = new HashSet<>();
		for (Field f : fields) {
			if (Modifier.isPublic(f.getModifiers()) && Modifier.isStatic(f.getModifiers())) {
				addIfAllowed(s, f.getType());
			}
		}
		return s;
	}

	private <E extends Executable> Set<Class> getImmediateReferences(E[] _e) {
		Set<Class>	s;
		
		s = new HashSet<>();
		for (E e : _e) {
			if (Modifier.isPublic(e.getModifiers())) {
				s.addAll(getImmediateReferences(e));
			}
		}
		return s;
	}	
	
	private <E extends Executable> Set<Class> getImmediateReferences(E e) {
		Set<Class>	s;
		
		s = new HashSet<>();
		if (e instanceof Method && Modifier.isPublic(e.getModifiers())) {
			addIfAllowed(s, ((Method)e).getReturnType());
		}
		for (Parameter p : e.getParameters()) {
			addIfAllowed(s, p.getType());
		}
		return s;
	}
	
	////////////////////////////////////////////////////////////////////////////////
	
	private static boolean excluded(String name, Set<String> exclusionFilters) {
		for (String filter : exclusionFilters) {
			System.out.printf("\t%s %s %s\n", name, filter, name.matches(filter));
			if (name.matches(filter)) {
				return true;
			}
		}
		return false;
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

	public boolean referencePresent(Class c) {
		return references.contains(c);
	}
	
	public boolean allReferencesPresent(Executable e) {
		System.out.printf("Executable %s\n", e.getName());
		for (Parameter p : e.getParameters()) {
			if (!references.contains(p.getType()) && !Variable.isMappedJavaType(p.getType().getName())) {
				System.out.printf("\tMissing reference for parameter %s\n", p.getType());
				return false;
			}
		}
		if (e instanceof Method) {
			if (!references.contains(((Method)e).getReturnType()) && !Variable.isMappedJavaType(((Method)e).getReturnType().getName())) {
				System.out.printf("\tMissing reference for return type %s\n", ((Method)e).getReturnType());
				return false;
			}
		}
		return true;
	}
	
	public static void main(String[] args) {
		ClassReferenceFinder	crf;
		
		crf = new ClassReferenceFinder(GenPackageClasses.createForPackagesAndClasses(new File(args[0]), args[1]));
		crf.addReferencedClasses();
		System.out.printf("%s\n", CollectionUtil.toString(crf.getReferences()));
	}	
}
