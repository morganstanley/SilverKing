package com.ms.silverking.cloud.dht.client.gen;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

public class Context {
	private Package		_package;
	private Class		_class;
	private Method		_method;
	private Parameter	_parameter;
	private String		outputFileName;
	private PrintStream	outStream;
	
	public Context() {
	}
	
	public Package getCurrentPackage() {
		return _package;
	}

	public void setCurrentPackage(Package _package) {
		this._package = _package;
	}

	public Class getCurrentClass() {
		return _class;
	}

	public void setCurrentClass(Class _class) {
		this._class = _class;
	}

	public Method getCurrentMethod() {
		return _method;
	}

	public void setCurrentMethod(Method _method) {
		this._method = _method;
	}

	public Parameter getCurrentParameter() {
		return _parameter;
	}

	public void setCurrentParameter(Parameter _parameter) {
		this._parameter = _parameter;
	}

	public String getCurrentOutputFileName() {
		return outputFileName;
	}

	public void setCurrentOutputFileName(String outputFileName) {
		this.outputFileName = outputFileName;
		try {
			switchOutStream(outputFileName);
		} catch (FileNotFoundException fnfe) {
			throw new RuntimeException("Can't switch to new output file: "+ outputFileName, fnfe);
		}
	}
	
	public PrintStream getOutStream() {
		return outStream;
	}

	private void switchOutStream(String newOutputFileName) throws FileNotFoundException {
		closeOutStream();
		outStream = new PrintStream(new FileOutputStream(newOutputFileName));
	}
	
	private void closeOutStream() {
		outStream.close();
	}

	public void close() {
		if (outStream != null) {
			closeOutStream();
		}
	}
}
