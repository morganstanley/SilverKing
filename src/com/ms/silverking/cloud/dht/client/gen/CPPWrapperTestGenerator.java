package com.ms.silverking.cloud.dht.client.gen;

import java.io.PrintStream;
import java.lang.reflect.Method;

public class CPPWrapperTestGenerator extends CPPGeneratorBase {
	public CPPWrapperTestGenerator(PrintStream out) {
		super(out);
	}
	
	public CPPWrapperTestGenerator() {
		this(System.out);
	}
	
	///////////////////////////////////////
	
	private String testMethod(Class c) {
		return String.format("test_%s", c.getSimpleName());
	}
	
	///////////////////////////////////////
	
	public void generateHeader(Class[] classes) {
		p("#include <stdio.h>");
		p("#include <jni.h>");
		for (Class c : classes) {
			p("#include \"%s.h\"", c.getSimpleName());
		}
		p();
	}
	
	private void instantiate(Class c) {
		p("\t%s::%s\t*o = new %s::%s();", getNamespaceCPPEquivalent(c), c.getSimpleName(), getNamespaceCPPEquivalent(c), c.getSimpleName());
	}
	
	private void destroy(Class c) {
		p("\tdelete o;");
	}
	
	private void generateMethodTest(Class c, Method m) {
	}
	
	private void generateClassTests(Class c) {
		p("void %s() {", testMethod(c));
		p("\tprintf(\"Testing:\t%s\\n\");", c.getSimpleName());
		instantiate(c);
		for (Method m : c.getMethods()) {
			generateMethodTest(c, m);
		}
		destroy(c);
		p("\tprintf(\"Test complete:\t%s\\n\");", c.getSimpleName());
		p("}");
	}
	
	public void generateInitJVM() {
		p("int initJVM() {");
		
		
		
		/*
		p("\tJavaVMOption jvmopt[3];");
		p();
		p("\tjvmopt[0].optionString = \"printf\";");
		p("\tjvmopt[0].extraInfo = jvmMsgRedirection_hook;");
		p();
		p("\tjvmopt[1].optionString = \"abort\";");
		p("\tjvmopt[1].extraInfo = jvmAbort_hook;");
		p();
		p("\tjvmopt[2].optionString = \"exit\";");
		p("\tjvmopt[2].extraInfo = jvmExit_hook;");		
		p();
		*/
		
		p("\tJNIEnv *jniEnv;");
		p("\tJavaVM *javaVM;");
		p("\tlong flag;");
		p("\tflag = JNI_CreateJavaVM(&javaVM, (void**)&jniEnv, &vm_args);");
		p("\tif (flag == JNI_ERR) {");
		p("\t\tprintf(\"Error creating JVM\\n\");");
		p("\t\treturn 1;");
		p("\t} else {");
		p("\t\tprintf(\"JVM Created\\n\");");
		p("\t\treturn 0;");
		p("\t}");
		p("}");
		p();
	}
	
	public void generateMain(Class[] classes) {
		p();
		p("int main(int argc, char **argv) {");
		p("\tinitJVM();");
		for (Class c : classes) {
			p("\t%s();", testMethod(c));
		}
		p("}");
		p();
	}
	
	public void generateAll(Class[] classes) {
		generateHeader(classes);
		generateInitJVM();
		for (Class c : classes) {
			generateClassTests(c);
		}
		generateMain(classes);
	}
	
	public static void main(String[] args) {
		try {
			if (args.length < 1) {
				System.out.println("args: <className...>");
			} else {
				CPPWrapperTestGenerator	tg;
				Class[]				classes;
			
				classes = new Class[args.length];
				for (int i = 0; i < classes.length; i++) {
					classes[i] = Class.forName(args[i]);
				}
				tg = new CPPWrapperTestGenerator();
				tg.generateAll(classes);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
