package com.ms.silverking.cloud.dht.client.gen;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.ms.silverking.collection.HashedListMap;

/**
 * Contains a Package and the Classes to generate wrappers for within that package
 */
public class GenPackageClasses {
	private final Package		_package;
	private final List<Class>	classes;
	
	public GenPackageClasses(Package _package, List<Class> classes) {
		this._package = _package;
		this.classes = classes;
	}
	
	public Package getPackage() {
		return _package;
	}
	
	public List<Class> getClasses() {
		return classes;
	}
	
	private static List<Class> getAllClassesInPackage(Package _package, File codebase) {
		File		packageDir;
		List<Class>	classes;
		File[]		candidates;
		
		packageDir = new File(codebase.toPath() +"/"+ _package.getName().replace('.', '/'));
		candidates = packageDir.listFiles();
		classes = new ArrayList<>();
		for (File candidate : candidates) {
			if (candidate.getName().endsWith(".java")) {
				String	className;
				Class	_class;
				int	i;
				
				i = candidate.getName().indexOf(".java");
				className = _package.getName() +"."+ candidate.getName().substring(0, i);
				try {
					_class = Class.forName(className);
					classes.add(_class);
				} catch (ClassNotFoundException cnfe) {
					System.err.printf("Couldn't find: %s\n", className);
				}
			}
		}
		return classes;
	}
	
	public String toString() {
		StringBuffer	sb;
		
		sb = new StringBuffer();
		sb.append(_package.getName() +":");
		for (Class _class : classes) {
			sb.append(_class.getName() +",");
		}
		return sb.toString();
	}
	
	public static List<GenPackageClasses> createForPackagesAndClasses(File codebase, String packagesAndClasses) {
		String[]	_packagesAndClasses;
		HashedListMap<String,String>	packageDefs;
		List<GenPackageClasses>	packageClassesList;
		
		_packagesAndClasses = packagesAndClasses.split(",");
		packageDefs = new HashedListMap<>();
		for (String s : _packagesAndClasses) {
			String	packageName;
			String	classNameCandidate;
			
			classNameCandidate = s.substring(s.lastIndexOf('.') + 1);
			if (Character.isUpperCase(classNameCandidate.charAt(0))) {
				packageName = s.substring(0, s.lastIndexOf('.'));
				packageDefs.addValue(packageName, classNameCandidate);
			} else {
				packageName = classNameCandidate;
				packageDefs.addValue(packageName, "");
			}
		}
		packageClassesList = new ArrayList<>();
		for (String packageName : packageDefs.getKeys()) {
			List<String>	classNames;
			
			classNames = new ArrayList<>();
			for (String className : packageDefs.getList(packageName)) {
				if (className.length() > 0) {
					classNames.add(className);
				}
			}
			packageClassesList.add(GenPackageClasses.create(codebase, packageName, classNames));
		}
		return packageClassesList;
	}
	
	public static GenPackageClasses create(File codebase, String packageName) {
		return create(codebase, packageName, null);
	}
	
	public static GenPackageClasses create(File codebase, String packageName, List<String> classNames) {
		Package		_package;
		List<Class>	classes;
		
		_package = Package.getPackage(packageName);
		if (classNames == null || classNames.size() == 0) {
			classes = getAllClassesInPackage(_package, codebase);
		} else {
			classes = new ArrayList<>();
			for (String className : classNames) {
				try {
					classes.add(Class.forName(className));
				} catch (ClassNotFoundException cnfe) {
					throw new RuntimeException(cnfe);
				}
			}
		}
		return new GenPackageClasses(_package, classes);
	}
	
	public static void main(String[] args) {
		try {
			GenPackageClasses	g;
			
			g = create(new File(args[0]), args[1]);
			System.out.println(g);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
