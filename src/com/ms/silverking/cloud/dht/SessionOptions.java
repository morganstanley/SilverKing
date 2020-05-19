package com.ms.silverking.cloud.dht;

import java.util.HashMap;
import java.util.Map;

import com.ms.silverking.cloud.dht.client.ClientDHTConfiguration;
import com.ms.silverking.cloud.dht.client.ClientDHTConfigurationProvider;
import com.ms.silverking.cloud.dht.client.SessionEstablishmentTimeoutController;
import com.ms.silverking.cloud.dht.client.SimpleSessionEstablishmentTimeoutController;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.text.FieldsRequirement;
import com.ms.silverking.text.ObjectDefParser2;
import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.PropertiesHelper.UndefinedAction;

/**
 * Options for a DHTSession.
 */
public final class SessionOptions {
  public static final String EMBEDDED_PASSIVE_NODE = "__EMBEDDED_PASSIVE_NODE__";
  public static final String EMBEDDED_KVS = "__EMBEDDED_KVS__";

  private final ClientDHTConfiguration dhtConfig;
  private final String preferredServer;
  private final SessionEstablishmentTimeoutController timeoutController;

  private static final String defaultTimeoutControllerProperty =
      SessionEstablishmentTimeoutController.class.getName() + ".DefaultSETimeoutController";
  private static final SessionEstablishmentTimeoutController defaultDefaultTimeoutController =
      new SimpleSessionEstablishmentTimeoutController(
      14, 2 * 60 * 1000, 8 * 60 * 1000);

  private static SessionEstablishmentTimeoutController defaultTimeoutController;
  private static final boolean debugDefaultTimeoutController = false;

  private static final Map<String, String> dummyGCMap;

  static {
    dummyGCMap = new HashMap<>();
    dummyGCMap.put(ClientDHTConfiguration.nameVar, "dummyname");
    dummyGCMap.put(ClientDHTConfiguration.portVar, "80");
    dummyGCMap.put(ClientDHTConfiguration.zkLocVar, "localhost:0");
  }

  private static final SessionOptions template = new SessionOptions(new SKGridConfiguration("dummygc", dummyGCMap),
      "localhost", defaultDefaultTimeoutController);

  static {
    ObjectDefParser2.addParser(template, FieldsRequirement.ALLOW_INCOMPLETE);
  }

  static {
    String def;

    def = PropertiesHelper.systemHelper.getString(defaultTimeoutControllerProperty, UndefinedAction.ZeroOnUndefined);
    if (debugDefaultTimeoutController) {
      System.out.printf("defaultTimeoutControllerProperty %s\n", defaultTimeoutControllerProperty);
      System.out.printf("def %s\n", def);
    }
    if (def != null) {
      //defaultTimeoutController = SimpleConnectionEstablishmentTimeoutController.parse(def);
      defaultTimeoutController = ObjectDefParser2.parse(SessionEstablishmentTimeoutController.class, def);
    } else {
      defaultTimeoutController = defaultDefaultTimeoutController;
    }
    if (debugDefaultTimeoutController) {
      System.out.printf("defaultTimeoutController %s\n", defaultTimeoutController);
    }
  }

  public static boolean isReservedServerName(String serverName) {
    return serverName != null && (serverName.equals(EMBEDDED_KVS) || serverName.equals(EMBEDDED_PASSIVE_NODE));
  }

  public void setDefaultTimeoutController(SessionEstablishmentTimeoutController newDefaultTimeoutController) {
    defaultTimeoutController = newDefaultTimeoutController;
  }

  public static SessionEstablishmentTimeoutController getDefaultTimeoutController() {
    return defaultTimeoutController;
  }

  /**
   * Create a fully-specified SessionOptions instance
   *
   * @param dhtConfigProvider TODO
   * @param preferredServer   TODO
   * @param timeoutController TODO
   */
  public SessionOptions(ClientDHTConfigurationProvider dhtConfigProvider, String preferredServer,
      SessionEstablishmentTimeoutController timeoutController) {
    if (dhtConfigProvider == null) {
      this.preferredServer = preferredServer;
      this.dhtConfig = null;
    } else {
      this.dhtConfig = dhtConfigProvider.getClientDHTConfiguration();
      if (dhtConfig.getName().equals(SessionOptions.EMBEDDED_KVS)) {
        this.preferredServer = SessionOptions.EMBEDDED_KVS;
      } else {
        this.preferredServer = preferredServer;
      }
    }
    this.timeoutController = timeoutController;
  }

  /**
   * Create a SessionOptions instance with a default timeout controller.
   *
   * @param dhtConfigProvider TODO
   * @param preferredServer   TODO
   */
  public SessionOptions(ClientDHTConfigurationProvider dhtConfigProvider, String preferredServer) {
    this(dhtConfigProvider, preferredServer, getDefaultTimeoutController());
  }

  /**
   * Create a SessionOptions instance with the default preferredServer and timeout controller
   *
   * @param dhtConfigProvider TODO
   */
  public SessionOptions(ClientDHTConfigurationProvider dhtConfigProvider) {
    this(dhtConfigProvider, null);
  }

  /**
   * Return a new SessionOptions object with the specified dhtConfig
   *
   * @param dhtConfig the new dhtConfig
   * @return a modified SessionOptions object
   */
  public SessionOptions dhtConfig(ClientDHTConfiguration dhtConfig) {
    return new SessionOptions(dhtConfig, preferredServer, timeoutController);
  }

  /**
   * Return a new SessionOptions object with the specified preferredServer
   *
   * @param preferredServer the new preferredServer
   * @return a modified SessionOptions object
   */
  public SessionOptions preferredServer(String preferredServer) {
    return new SessionOptions(dhtConfig, preferredServer, timeoutController);
  }

  /**
   * Return a new SessionOptions object with the specified timeoutController
   *
   * @param timeoutController the new timeoutController
   * @return a modified SessionOptions object
   */
  public SessionOptions timeoutController(SessionEstablishmentTimeoutController timeoutController) {
    return new SessionOptions(dhtConfig, preferredServer, timeoutController);
  }

  /**
   * Return the ClientDHTConfiguration
   *
   * @return the ClientDHTConfiguration
   */
  public ClientDHTConfiguration getDHTConfig() {
    return dhtConfig;
  }

  /**
   * Return the preferrsedServer
   *
   * @return the preferrsedServer
   */
  public String getPreferredServer() {
    return preferredServer;
  }

  /**
   * Return the timeoutController
   *
   * @return the timeoutController
   */
  public SessionEstablishmentTimeoutController getTimeoutController() {
    return timeoutController;
  }

  @Override
  public int hashCode() {
    return dhtConfig.hashCode() ^ preferredServer.hashCode() ^ timeoutController.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    SessionOptions o;

    o = (SessionOptions) obj;
    return this.dhtConfig.equals(o.dhtConfig) && this.preferredServer.equals(
        o.preferredServer) && this.timeoutController.equals(o.timeoutController);
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }

  public static SessionOptions parse(String def) {
    return ObjectDefParser2.parse(SessionOptions.class, def);
  }
}
