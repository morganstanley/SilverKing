package com.ms.silverking.cloud.dht.benchmark.ycsb;
import com.ms.silverking.cloud.dht.ConsistencyProtocol;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.RevisionMode;
import com.ms.silverking.cloud.dht.StorageType;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.client.OpSizeBasedTimeoutController;
import com.ms.silverking.cloud.dht.common.DHTConstants;

class SilverkingDBConstants {
	private static final int	secondarySyncIntervalSeconds = 1800;
	private static final int	segmentSize = 67108864;
	
    static final PutOptions putOptions = new PutOptions( new OpSizeBasedTimeoutController(),
    										DHTConstants.noSecondaryTargets, 
    										Compression.NONE,
                                            ChecksumType.NONE,
                                            false,
                                            PutOptions.defaultVersion,
                                            null
                                            );

    static final String namespace = "ycsb.1";
    static final NamespaceOptions    nsOptions_file = new NamespaceOptions(StorageType.FILE, 
                                                        ConsistencyProtocol.LOOSE, 
                                                        //ConsistencyProtocol.TWO_PHASE_COMMIT, 
                                                        NamespaceVersionMode.SYSTEM_TIME_NANOS, 
                                                        RevisionMode.NO_REVISIONS,
                                                        putOptions,//.compression(Compression.LZ4),
                                                        DHTConstants.standardInvalidationOptions,
                                                        DHTConstants.standardGetOptions,
                                                        DHTConstants.standardWaitOptions,
                                                        secondarySyncIntervalSeconds, segmentSize, false);
    static final NamespaceOptions    nsOptions_ram = new NamespaceOptions(StorageType.RAM, 
                                                        ConsistencyProtocol.LOOSE, 
                                                        //ConsistencyProtocol.TWO_PHASE_COMMIT, 
                                                        NamespaceVersionMode.SYSTEM_TIME_NANOS,
                                                        RevisionMode.NO_REVISIONS,
                                                        putOptions,
                                                        DHTConstants.standardInvalidationOptions,
                                                        DHTConstants.standardGetOptions,
                                                        DHTConstants.standardWaitOptions,
                                                        secondarySyncIntervalSeconds, segmentSize, false);
    static final NamespaceOptions    nsOptions = nsOptions_file;
}
