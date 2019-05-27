package com.ms.silverking.test.pingpong;

import java.util.ArrayList;
import java.util.List;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

public class PingPongOptions {
    public PingPongOptions() {
    }
    
    @Option(name="-m", usage="mode", required=true)
    public Mode    mode;
    
    @Option(name="-g", usage="GridConfig", required=true)
    public String   gridConfig;
    
    @Option(name="-s", usage="server")
    public String   server;
    
    @Option(name="-n", usage="namespace")
    public String   namespace;
    
    @Option(name="-S", usage="numServers")
    public int  numServers = 1;
    
    @Option(name="-T", usage="threadsPerServer")
    public int  threadsPerServer = 1;
    
    @Option(name="-noValidation", usage="turn off checksum validation")
    public boolean noValidation;
    
    @Option(name="-i", usage="iterations")
    public int  iterations = 10;
    
    @Option(name="-I", usage="id")
    public int  id;
    
    @Option(name="-d", usage="delay")
    public int  delay = 10;
    
    @Option(name="-c", usage="consistencyMode")
    public String   consistencyMode;
    
    @Option(name="-C", usage="checksumType")
    public String  checksumType;
    
    @Option(name="-V", usage="verbose")
    public boolean verbose;
    
    @Argument
    public List<String> arguments = new ArrayList<String>();
}
