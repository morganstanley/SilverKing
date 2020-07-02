package com.ms.silverking.cloud.dht.client;

import com.ms.silverking.cloud.dht.common.NamespaceOptionsMode;
import com.ms.silverking.cloud.dht.meta.DHTConfiguration;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.util.PropertiesHelper;

import java.util.HashMap;
import java.util.Map;

public class EmbeddedSKConfiguration {
  private final String dhtName;
  private final String gridConfigName;
  private final int dhtPort;
  private final String ringName;
  private final int replication;
  private final Map<String, String> classVars;
  private final NamespaceOptionsMode namespaceOptionsMode;
  private final boolean enableMsgGroupTrace;

  private static final String defaultInstanceNamePrefix = "SK.";
  private static final String defaultGridConfigNamePrefix = "GC_SK_";
  private static final String defaultRingNamePrefix = "ring.";
  private static final int defaultReplication = 1;

  public static final String skPortProperty = EmbeddedSK.class.getName() + ".SKPort";
  public static final int defaultSKPort = 0;
  private static final int skPort;
  // Allows the node to start aliased to the below address rather than the host IP
  // This can be used e.g. to set the interface to be 127.0.0.1 to keep connections strictly local
  // in combination with IPAddrUtil ipProperty system flag. Typically this field will be null
  private final String daemonIp;

  static {
    skPort = PropertiesHelper.systemHelper.getInt(skPortProperty, defaultSKPort,
        PropertiesHelper.ParseExceptionAction.RethrowParseException);
  }

  public EmbeddedSKConfiguration(String dhtName, String gridConfigName, int dhtPort, String ringName, int replication,
      Map<String, String> classVars, NamespaceOptionsMode namespaceOptionsMode, boolean enableMsgGroupTrace, String daemonIp) {
    this.dhtName = dhtName;
    this.gridConfigName = gridConfigName;
    this.dhtPort = dhtPort;
    this.ringName = ringName;
    this.replication = replication;
    this.classVars = classVars;
    this.namespaceOptionsMode = namespaceOptionsMode;
    this.enableMsgGroupTrace = enableMsgGroupTrace;
    this.daemonIp = daemonIp;
  }

  public EmbeddedSKConfiguration(String id, int replication) {
    this(defaultInstanceNamePrefix + id, defaultGridConfigNamePrefix + id, skPort, defaultRingNamePrefix + id,
        replication, new HashMap<String, String>(), DHTConfiguration.defaultNamespaceOptionsMode,
        DHTConfiguration.defaultEnableMsgGroupTrace, null);
  }

  public EmbeddedSKConfiguration(String id) {
    this(defaultInstanceNamePrefix + id, defaultGridConfigNamePrefix + id, skPort, defaultRingNamePrefix + id,
        defaultReplication, new HashMap<String, String>(), DHTConfiguration.defaultNamespaceOptionsMode,
        DHTConfiguration.defaultEnableMsgGroupTrace, null);
  }

  public int getDhtPort() {
    return dhtPort;
  }

  public EmbeddedSKConfiguration(int replication) {
    this(new UUIDBase(false).toString(), replication);
  }

  public EmbeddedSKConfiguration() {
    this(new UUIDBase(false).toString());
  }

  public String getDHTName() {
    return dhtName;
  }

  public String getGridConfigName() {
    return gridConfigName;
  }

  public String getRingName() {
    return ringName;
  }

  public int getReplication() {
    return replication;
  }

  public Map<String, String> getClassVars() {
    return classVars;
  }

  public NamespaceOptionsMode getNamespaceOptionsMode() {
    return namespaceOptionsMode;
  }

  public boolean getEnableMsgGroupTrace() {
    return enableMsgGroupTrace;
  }

  public String getDaemonIp(){
    return daemonIp;
  }

  public EmbeddedSKConfiguration dhtName(String dhtName) {
    System.out.printf("dhtName %s \n", dhtName);
    return new EmbeddedSKConfiguration(dhtName, gridConfigName, dhtPort, ringName, replication, classVars,
        namespaceOptionsMode, enableMsgGroupTrace, daemonIp);
  }

  public EmbeddedSKConfiguration gridConfigName(String gridConfigName) {
    return new EmbeddedSKConfiguration(dhtName, gridConfigName, dhtPort, ringName, replication, classVars,
        namespaceOptionsMode, enableMsgGroupTrace, daemonIp);
  }

  public EmbeddedSKConfiguration ringName(String ringName) {
    return new EmbeddedSKConfiguration(dhtName, gridConfigName, dhtPort, ringName, replication, classVars,
        namespaceOptionsMode, enableMsgGroupTrace, daemonIp);
  }

  public EmbeddedSKConfiguration dhtPort(int dhtPort) {
    return new EmbeddedSKConfiguration(dhtName, gridConfigName, dhtPort, ringName, replication, classVars,
        namespaceOptionsMode, enableMsgGroupTrace, daemonIp);
  }

  public EmbeddedSKConfiguration replication(int replication) {
    return new EmbeddedSKConfiguration(dhtName, gridConfigName, dhtPort, ringName, replication, classVars,
        namespaceOptionsMode, enableMsgGroupTrace, daemonIp);
  }

  public EmbeddedSKConfiguration classVars(Map<String, String> classVars) {
    return new EmbeddedSKConfiguration(dhtName, gridConfigName, dhtPort, ringName, replication, classVars,
        namespaceOptionsMode, enableMsgGroupTrace, daemonIp);
  }

  public EmbeddedSKConfiguration namespaceOptionsMode(NamespaceOptionsMode namespaceOptionsMode) {
    return new EmbeddedSKConfiguration(dhtName, gridConfigName, dhtPort, ringName, replication, classVars,
        namespaceOptionsMode, enableMsgGroupTrace, daemonIp);
  }

  public EmbeddedSKConfiguration enableMsgGroupTrace(boolean enableMsgGroupTrace) {
    return new EmbeddedSKConfiguration(dhtName, gridConfigName, dhtPort, ringName, replication, classVars,
        namespaceOptionsMode, enableMsgGroupTrace, daemonIp);
  }

  public EmbeddedSKConfiguration daemonIp(String daemonIp) {
    return new EmbeddedSKConfiguration(dhtName, gridConfigName, dhtPort, ringName, replication, classVars,
        namespaceOptionsMode, enableMsgGroupTrace, daemonIp);
  }
}
