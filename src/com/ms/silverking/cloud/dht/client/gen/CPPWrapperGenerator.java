package com.ms.silverking.cloud.dht.client.gen;

import java.io.PrintStream;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

public class CPPWrapperGenerator extends CPPGeneratorBase {
	private enum Mode {Definition, Implementation};
	
	public CPPWrapperGenerator(PrintStream out) {
		super(out);
	}
	
	public CPPWrapperGenerator() {
		this(System.out);
	}

	//////////////////////////////////////////
	
	public void generate(Class c, Mode mode) {
		generateHeader(c, mode);
		generateBody(c, mode);
		generateFooter(c, mode);
	}
	
	private void generateHeader(Class c, Mode mode) {
		p("// %s.%s", c.getName(), mode == Mode.Definition ? "h" : "cpp");
		p();
		if (mode == Mode.Definition) {
			p("#ifndef "+ ifndef(c));
			p("#define "+ ifndef(c));
			p();
			p("namespace %s {", getNamespaceCPPEquivalent(c));
			p();
			//p("class %s %s{", c.getSimpleName(), classExtension(c));
			p("class %s {", c.getSimpleName()); // FIXME TEMP
			p("private:");
		} else {
			p("#include <jni.h>");
			p("#include \"%s.h\"", c.getSimpleName());
			p("#include \"SKWrapperCommon.h\"");
			p();
			p("using namespace %s;", getNamespaceCPPEquivalent(c));
			p();
		}
	}
	
	private String classExtension(Class c) {
		Class	s;
		StringBuffer	sb;
		boolean	isFirst;
		
		isFirst = true;
		sb = new StringBuffer();
		s = c.getSuperclass();
		if (s != null) {
			sb.append(String.format(": public %s ", s.getSimpleName()));
			isFirst = false;
		}
		for (Class si : c.getInterfaces()) {
			sb.append(String.format("%s public %s ", isFirst ? ":" : ",", si.getSimpleName()));
		}
		return sb.toString();
	}

	private void generateBody(Class c, Mode mode) {
		generateFields(c, mode);
		if (mode == Mode.Definition) {
			p("public:");
		}
		generateConstructor(c, mode);
		generateDestructor(c, mode);
		generateMethods(c, mode);
	}

	private void generateConstructor(Class c, Mode mode) {
		if (mode == Mode.Definition) {
			p("\t%s();", c.getSimpleName());
		} else {
			p("%s::%s() {", c.getSimpleName(), c.getSimpleName());
			p("\tjclass _class = skw_jenv->FindClass(\"%s\");", c.getName().replace('.', '/'));
			generateMethodIDs(c);
			p("}", c.getSimpleName());
			p();
		}
	}

	private void generateMethodIDs(Class c) {
		for (Method m : c.getMethods()) {
			if (m.getDeclaringClass().equals(c)) {
				generateMethodID(m);
			}
		}
	}
	
	private void generateMethodID(Method m) {
		p("\t%s = skw_jenv->GetMethodID(_class, \"%s\", \"%s\");", getMethodFieldName(m), m.getName(), getMethodSignature(m));
	}

	private void generateDestructor(Class c, Mode mode) {
		if (mode == Mode.Definition) {
			p("\t~%s();", c.getSimpleName());
		} else {
			p("%s::~%s() {", c.getSimpleName(), c.getSimpleName());
			p("}", c.getSimpleName());
			p();
		}
	}
	
	private void generateMethods(Class c, Mode mode) {
		for (Method m : c.getMethods()) {
			if (m.getDeclaringClass().equals(c)) {
				generateMethod(c, m, mode);
			}
		}
	}

	private void generateMethod(Class c, Method m, Mode mode) {
		if (mode == Mode.Definition) {
			p("\t%s %s(%s);", m.getReturnType(), m.getName(), getMethodSignature(m, mode));
		} else {
			p("%s %s::%s(%s) {", m.getReturnType(), c.getSimpleName(), m.getName(), getMethodSignature(m, mode));
			invokeMethod(m);
			p("}");
			p();
		}
	}
	
	private void invokeMethod(Method m) {
		p("\tskw_jenv->CallXXXXMethod(instance, _class, %s, %s);", getMethodFieldName(m), getMethodParameters(m));
	}

	private void generateFields(Class c, Mode mode) {
		for (Method m : c.getMethods()) {
			if (m.getDeclaringClass().equals(c)) {
				generateFieldsForMethod(c, m, mode);
			}
		}
	}

	private void generateFieldsForMethod(Class c, Method m, Mode mode) {
		if (mode == Mode.Definition) {
			p("\tjmethodID\t%s;", getMethodFieldName(m));
		} else {
		}
	}
	
	private String typeToField(Class t) {
		String	n;
		
		n = t.getName();
		n = n.replace('[', '_');
		return n;
	}
	
	private String getMethodFieldName(Method m) {
		StringBuffer	sb;
		boolean			isFirst;
		
		sb = new StringBuffer();
		sb.append("id_"+ m.getName());
		isFirst = true;
		for (Parameter p : m.getParameters()) {
			if (!isFirst) {
				sb.append("__");
			}
			sb.append(typeToField(p.getType()));
			isFirst = false;
		}
		return sb.toString();
	}
	
	private String getMethodSignature(Method m) {
		StringBuffer	sb;
		boolean			isFirst;
		
		sb = new StringBuffer();
		isFirst = true;
		for (Parameter p : m.getParameters()) {
			if (!isFirst) {
				sb.append(";");
			}
			sb.append(p.getType().getName());
			isFirst = false;
		}
		return sb.toString();
	}
	
	private String getMethodSignature(Method m, Mode mode) {
		StringBuffer	sb;
		boolean			isFirst;
		
		sb = new StringBuffer();
		isFirst = true;
		for (Parameter p : m.getParameters()) {
			if (!isFirst) {
				sb.append(", ");
			}
			sb.append(p.getType().getName());
			sb.append(' ');
			sb.append(p.getName());
			isFirst = false;
		}
		return sb.toString();
	}

	private String getMethodParameters(Method m) {
		StringBuffer	sb;
		boolean			isFirst;
		
		sb = new StringBuffer();
		isFirst = true;
		for (Parameter p : m.getParameters()) {
			if (!isFirst) {
				sb.append(", ");
			}
			sb.append(p.getName());
			isFirst = false;
		}
		return sb.toString();
	}
	
	private void generateFooter(Class c, Mode mode) {
		if (mode == Mode.Definition) {
			p("};"); // class
			p("}"); // namespace
			p();
			p("#endif // %s", ifndef(c));
		}
		p();
	}
	
	public static void main(String[] args) {
		try {
			if (args.length < 1 || args.length > 2) {
				System.out.println("args: <className> <mode>");
			} else {
				CPPWrapperGenerator	wg;
				String				className;
				Mode				mode;
			
				wg = new CPPWrapperGenerator();
				className = args[0];
				if (args.length == 2) {
					mode = Mode.valueOf(args[1]);
					wg.generate(Class.forName(className), mode);
				} else {
					wg.generate(Class.forName(className), Mode.Definition);
					wg.generate(Class.forName(className), Mode.Implementation);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
