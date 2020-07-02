package com.ms.silverking.cloud.dht.daemon.storage;

public class NamespaceStoreConstants {
  public class SystemConstants {
    public static final String totalDiskBytesVar = "totalDiskBytes";
    public static final String usedDiskBytesVar = "usedDiskBytes";
    public static final String freeDiskBytesVar = "freeDiskBytes";
    public static final String diskBytesVar = "diskBytes";
    public static final String allReplicasVar = "allReplicas";
    public static final String allReplicasFreeDiskBytesVar = "allReplicasFreeDiskBytes";
    public static final String allReplicasFreeSystemDiskBytesEstimateVar = "allReplicasFreeSystemDiskBytesEstimate";
    public static final String exclusionSetVar = "exclusionSet";
    public static final String ringHealthVar = "ringHealth";
  }
  
  public class NodeConstants {
    public static final String nodeIDVar = "nodeID";
    public static final String bytesFreeVar = "bytesFree";
    public static final String nsTotalKeysVar = "nsTotalKeys";
    public static final String nsBytesUncompressedVar = "nsBytesUncompressed";
    public static final String nsBytesCompressedVar = "nsBytesCompressed";
    public static final String nsTotalInvalidationsVar = "nsTotalInvalidations";
    public static final String nsTotalPutsVar = "nsTotalPuts";
    public static final String nsTotalRetrievalsVar = "nsTotalRetrievals";
  }
}
