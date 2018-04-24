package com.ms.silverking.cloud.dht.client.apps;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

class SilverKingClientOptions {
    private static final byte[] emptyValue = new byte[0];
    
	SilverKingClientOptions() {
	}
	
	@Option(name="-G", usage="GridConfigBase", required=false)
	String	gridConfigBase;
	
	@Option(name="-g", usage="GridConfig", required=false)
	String	gridConfig;
	
	@Option(name="-d", usage="clientDHTConfiguration", required=false)
	String	clientDHTConfiguration;
	
	@Option(name="-s", usage="server")
	String	server;
	
	@Option(name="-a", usage="action")
	Action action;
	
	@Option(name="-n", usage="namespace")
	String	namespace;
	
	@Option(name="-k", usage="key")
	String	key;
	
    @Option(name="-c", usage="commands")
    String  commands;
	
    @Option(name="-f", usage="commandFile")
    File    commandFile;
    
    //@Option(name="-N", usage="numKeys")
    //int  numKeys = 1;
	
	@Option(name="-v", usage="value")
	String	value;
    byte[]  valueBytes;
	
    @Option(name="-X", usage="randomValueSize")
    int  randomValueSize = -1;
    
    /*
	@Option(name="-noValidation", usage="turn off checksum validation")
	boolean noValidation;
	
	//@Option(name="-w", usage="warmup")
	//boolean warmup;
	
	@Option(name="-r", usage="reps")
	int	reps = 1;
	
    @Option(name="-e", usage="version")
    long version = PutOptions.defaultVersion;
    
    @Option(name="-m", usage="minVersion")
    long minVersion;
    
    @Option(name="-x", usage="maxVersion")
    long maxVersion;
    
    @Option(name="-C", usage="checksumType")
    ChecksumType    checksumType = ChecksumType.MD5;
	
    @Option(name="-V", usage="verbose")
    boolean verbose;
	
    @Option(name="-t", usage="timeoutSeconds")
    int timeoutSeconds;
    
    @Option(name="-T", usage="waitForTimeoutResponse")
    TimeoutResponse timeoutResponse;
    
    @Option(name="-z", usage="compression")
    Compression compression;
    
    @Option(name="-o", usage="namespaceOptions")
    String  nsOptions;
    */
    
    @Option(name="-l", usage="logLevel")
    String  logLevel = Level.WARNING.toString();
    
	@Argument
	List<String> arguments = new ArrayList<String>();
	
	@Override
	public String toString() {
		return ":"+ namespace +":"+ key +":";
	}
	
	private static final byte[]    randomChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0124356789".getBytes();
	
	private byte[] createRandomValue() {
	    ThreadLocalRandom  random;
	    byte[]             v;
	    
	    random = ThreadLocalRandom.current();
	    v = new byte[randomValueSize];
	    for (int i = 0; i < v.length; i ++) {
	        v[i] = randomChars[random.nextInt(randomChars.length)];
	    }
	    return v;
	}

    public byte[] getValue() {
        if (valueBytes == null) {
            if (randomValueSize >= 0 && value != null) {
                throw new RuntimeException("randomValueSize and value are mutually exclusive");
            } else {
                if (value != null) {
                    valueBytes = value.getBytes();
                } else if (randomValueSize > 0) {
                    valueBytes = createRandomValue();
                } else {
                    valueBytes = emptyValue;
                }
            }
        }
        return valueBytes;
    }
}