package com.ms.silverking.cloud.dht.client.gen;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.collection.HashedSetMap;
import com.ms.silverking.log.Log;

/**
 * Contains a Package, and the Classes to generate wrappers for within that package
 */
public class GenPackageClasses {
	private final Package		_package;
	private final List<Class>	classes;
	
	private static final boolean	debug = false;
	
	private static final String	classFileSuffix = ".class";
	
	public GenPackageClasses(Package _package, List<Class> classes) {
		assert _package != null;
		assert classes != null;
		this._package = _package;
		this.classes = classes;
	}
	
	public Package getPackage() {
		return _package;
	}
	
	public List<Class> getClasses() {
		return classes;
	}
	
	private static List<String> getAllClassesInPackageByFile(String _package, File codebase) {
		if (!codebase.exists()) {
			throw new RuntimeException("Can't find codebase: "+ codebase);
		} else {
			List<String>	classes;
			File			packageDir;

			packageDir = new File(codebase, _package.replace('.', '/'));
			classes = new ArrayList<>();
			for (String f : packageDir.list()) {
				if (f.endsWith(classFileSuffix)) {
					f = f.substring(0, f.length() - classFileSuffix.length());
					if (f.indexOf('$') < 0) {
						classes.add(f);
					} else {
						// Not a top-level class
					}
				} else {
					// Not a .class file
				}
			}
			return classes;
		}
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
		HashedSetMap<String,String>	packageDefs;
		
		_packagesAndClasses = packagesAndClasses.split(",");
		
		// First construct a HashedSetMap of packages, and classes in those packages
		packageDefs = new HashedSetMap<>();
		for (String s : _packagesAndClasses) {
			String	packageName;
			String	classNameCandidate;
			
			// SK convention: first character lower case for package names, upper case for classes 
			classNameCandidate = s.substring(s.lastIndexOf('.') + 1);
			if (Character.isUpperCase(classNameCandidate.charAt(0))) {
				packageName = s.substring(0, s.lastIndexOf('.'));
				packageDefs.addValue(packageName, classNameCandidate);
				System.out.printf("Adding package and class %s %s\n", packageName, classNameCandidate);
			} else {
				List<String>	packageClasses;
				
				packageName = s;
				//packageDefs.addValue(packageName, "");
				System.out.printf("Adding package %s\n", packageName);
				packageClasses = getAllClassesInPackageByFile(packageName, codebase);
				for (String c : packageClasses) {
					Class	_c;
					
					try {
						_c = Class.forName(packageName +"."+ c);
					} catch (ClassNotFoundException cnfe) {
						throw new RuntimeException(cnfe);
					}
					if (!JNIUtil.omitted(_c)) {
						System.out.printf("\tAdding class %s %s\n", packageName, c);
						packageDefs.addValue(packageName, c);
					}
				}
			}
		}
		return mapToList(packageDefs, codebase);
	}
	
	private static List<GenPackageClasses> mapToList(HashedSetMap<String,String> packageDefs, File codebase) {
		List<GenPackageClasses>	packageClassesList;
		
		System.out.println("\nCreating list of GenPackageClasses");
		//System.out.printf("codebase %s\n", codebase);
		
		System.out.println(packageDefs.getKeys().size());
		System.out.println(packageDefs.getNumKeys());
		System.out.println(packageDefs.getSets().size());
		
		// Now create a list of GenPackageClasses
		packageClassesList = new ArrayList<>();
		for (String packageName : packageDefs.getKeys()) {
			List<String>	classNames;
			
			System.out.printf("package %s\n", packageName);
			classNames = new ArrayList<>();
			for (String className : packageDefs.getSet(packageName)) {
				System.out.printf("\tclass %s\n", className);
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

	/**
	 * Get a package safely. Ensure that the classloader finds the package, then return it.
	 * @param packageName
	 * @return
	 */
	private static Package getPackage(File codebase, String packageName) {
		Package		_package;
		String		arbitraryPackageClassName;
		File		packageDir;
		String[]	classes;
		int			index;
		String	candidateClassName;
		
		packageDir = new File(codebase, packageName.replace('.', '/'));
		if (!packageDir.exists()) {
			throw new RuntimeException("Can't find packageDir: "+ packageDir);
		}
		classes = packageDir.list();
		if (classes.length == 0) {
			throw new RuntimeException("Can't find any classes in package: "+ packageDir);
		}
		candidateClassName = null;
		index = 0;
		arbitraryPackageClassName = null;
		while (arbitraryPackageClassName == null && index < classes.length) {
			candidateClassName = classes[index];
			if (candidateClassName.endsWith(".class")) {
				candidateClassName = candidateClassName.substring(0, candidateClassName.length() - ".class".length());
				try {
					Class	c;
					
					c = Class.forName(packageName +"."+ candidateClassName);
					arbitraryPackageClassName = candidateClassName;
				} catch (ClassNotFoundException e) {
					if (debug) {
						System.out.printf("candidateClassName failed: %s\n", candidateClassName);
					}
				}
			}				
			index++;
		}
		if (arbitraryPackageClassName == null) {
			throw new RuntimeException("Unable to find class in package: "+ packageName);
		}
		_package = Package.getPackage(packageName);
		return _package;
	}
	
	public static GenPackageClasses create(File codebase, String packageName, List<String> classNames) {
		Package		_package;
		List<Class>	classes;
		
		Log.warningf("%s %s %s", codebase, packageName, CollectionUtil.toString(classNames));
		_package = getPackage(codebase, packageName);
		if (_package == null) {
			throw new RuntimeException("Can't find package: "+ packageName);
		}
		if (classNames == null || classNames.size() == 0) {
			classes = getAllClassesInPackage(_package, codebase);
		} else {
			classes = new ArrayList<>();
			for (String className : classNames) {
				try {
					classes.add(Class.forName(packageName +"."+ className));
				} catch (ClassNotFoundException cnfe) {
					throw new RuntimeException(cnfe);
				}
			}
		}
		return new GenPackageClasses(_package, classes);
	}
	

	public static List<GenPackageClasses> add(List<GenPackageClasses> packageClassesList, Set<Class> references, File codebase) {
		HashedSetMap<String,String> packageDefs;
		
		packageDefs = new HashedSetMap<>();
		for (GenPackageClasses gpc : packageClassesList) {
			System.out.printf("Package: %s\n", gpc.getPackage());
			for (Class c : gpc.getClasses()) {
				System.out.printf("Adding: %s %s\n", gpc.getPackage(), c.getName());
				packageDefs.addValue(gpc.getPackage().getName(), c.getSimpleName());
			}
		}
		for (Class c : references) {
			System.out.printf("Adding reference: %s %s\n", c.getPackage().getName(), c.getName());
			packageDefs.addValue(c.getPackage().getName(), c.getSimpleName());
		}
		return mapToList(packageDefs, codebase);
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
