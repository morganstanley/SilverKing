package com.ms.silverking.cloud.management;

import java.util.ArrayList;
import java.util.List;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

public class MetaToolOptions {
    @Option(name="-t", usage="tool", required=true)
    public String  tool;
    @Option(name="-s", usage="source", required=true)
    public String  source;
    @Option(name="-d", usage="dest", required=true)
    public String  dest;
    @Option(name="-n", usage="name", required=true)
    public String  name;
    @Option(name="-z", usage="zkConfig", required=true)
    public String  zkConfig;
    @Option(name="-f", usage="file", required=false)
    public String  file;
    @Option(name="-v", usage="version", required=true)
    public String  version;
    
    @Argument
    public List<String> arguments = new ArrayList<String>();    
}
