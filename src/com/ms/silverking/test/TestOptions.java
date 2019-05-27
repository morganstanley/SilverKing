package com.ms.silverking.test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.TimeoutResponse;

public class TestOptions {
    public TestOptions() {
	}
	
	@Option(name="-g", usage="GridConfig", required=true)
	public String	gridConfig;
	
	@Option(name="-s", usage="server")
	public String	server;
	
	@Option(name="-a", usage="action", required=true)
	public String	action;
	
	@Option(name="-n", usage="namespace")
	public String	namespace;
	
	@Option(name="-k", usage="key")
	public String	key;
	
	@Option(name="-v", usage="value")
	public String	value;
	public byte[]  valueBytes;
	
    @Option(name="-X", usage="randomValueSize")
    public int  randomValueSize = -1;
    
	@Option(name="-noValidation", usage="turn off checksum validation")
	public boolean noValidation;
	
	@Option(name="-w", usage="warmup")
	public boolean warmup;
	
	@Option(name="-r", usage="reps")
	public int	reps = 1;
	
    @Option(name="-e", usage="version")
    public long version = PutOptions.defaultVersion;
    
    @Option(name="-m", usage="minVersion")
    public long minVersion;
    
    @Option(name="-x", usage="maxVersion")
    public long maxVersion;
    
	@Option(name="-c", usage="consistencyMode")
	public String	consistencyMode;
	
    @Option(name="-C", usage="checksumType")
    public String  checksumType;
	
    @Option(name="-V", usage="verbose")
    public boolean verbose;
	
    @Option(name="-t", usage="timeoutSeconds")
    public int timeoutSeconds;
    
    @Option(name="-T", usage="waitForTimeoutResponse")
    public TimeoutResponse timeoutResponse;
    
	@Argument
	public List<String> arguments = new ArrayList<String>();
	
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
                } else {
                    valueBytes = createRandomValue();
                }
            }
        }
        return valueBytes;
    }
}