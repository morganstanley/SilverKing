package com.ms.silverking.cloud.dht.client.apps.test;

import org.kohsuke.args4j.Option;

import com.ms.silverking.cloud.dht.ConsistencyProtocol;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.StorageType;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.Compression;

class BulkThroughputOptions {
	BulkThroughputOptions() {
	}
	
	@Option(name="-g", usage="GridConfig", required=true)
	String	gridConfig;
	
	@Option(name="-s", usage="server")
	String	server;
	
	@Option(name="-S", usage="storageType")
	StorageType    storageType = StorageType.FILE;
	
	@Option(name="-t", usage="test", required=true)
	BulkThroughputTest test;
	
	@Option(name="-i", usage="id", required=true)
	String	id;
	
    @Option(name="-I", usage="displayInterval", required=false)
    int  displayInterval = 100000;
    
    @Option(name="-n", usage="numKeys", required=true)
    int  numKeys = 1;
    
    @Option(name="-b", usage="batchSize", required=true)
    int  batchSize = 1;
    
    @Option(name="-v", usage="valueSize", required=true)
    int  valueSize = -1;
    
    @Option(name="-w", usage="clientWorkUnit")
    int clientWorkUnit = 10;
	
	@Option(name="-r", usage="reps")
	int	reps = 1;
	
    @Option(name="-x", usage="externalReps")
    int externalReps = 1;
    
	@Option(name="-c", usage="consistencyProtocol")
	ConsistencyProtocol    consistencyProtocol;
	
    @Option(name="-C", usage="checksumType")
    ChecksumType    checksumType = ChecksumType.NONE;
	
    @Option(name="-V", usage="verbose")
    boolean verbose = false;
	
    @Option(name="-D", usage="debug")
    boolean debug = false;
    
    // FUTURE - wire this up
    @Option(name="-z", usage="compression")
    Compression compression = Compression.NONE;
    
    @Option(name="-p", usage="parallelThreads")
    int parallelThreads = 1;
    
    @Option(name="-d", usage="dedicatedNamespaces")
    boolean dedicatedNamespaces = false;
    
    @Option(name="-m", usage="versionMode")
    NamespaceVersionMode    nsVersionMode;
    
    @Option(name="-verify", usage="verifyValues")
    boolean verifyValues = false;
    
    //@Option(name="-o", usage="namespaceOptions")
    //String  nsOptions;
    
	@Override
	public String toString() {
		return String.format("valueSize %d batchSize %d", valueSize, batchSize);
	}
}