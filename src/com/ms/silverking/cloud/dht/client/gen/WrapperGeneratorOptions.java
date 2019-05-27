package com.ms.silverking.cloud.dht.client.gen;

import org.kohsuke.args4j.Option;

class WrapperGeneratorOptions {
	WrapperGeneratorOptions() {
	}
	
	@Option(name="-c", usage="codebase", required = true)
	String	codebase;
	
	@Option(name="-i", usage="inputPackagesAndClasses", required = true)
	String	inputPackagesAndClasses;
	
	@Option(name="-f", usage="templateFile", required = true)
	String	templateFile;
	
	@Option(name="-o", usage="outputDir")
	String	outputDir = ".";
	
	@Option(name="-sd", usage="startDelimiter")
	String	startDelimiter = "{{";
	
	@Option(name="-ed", usage="endDelimiter")
	String	endDelimiter = "}}";
	
	@Option(name="-tm", usage="typeMappingFile")
	String	typeMappingFile;
	
	@Option(name="-d", usage="addDependencies")
	boolean	addDependencies;
}