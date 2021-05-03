package com.ms.silverking.cloud.dht.daemon;

import com.ms.silverking.net.async.ConnectionController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DisconnectAllExclusionResponder implements SelfExclusionResponder {

  ConnectionController connectionController;
  private static Logger log = LoggerFactory.getLogger(DisconnectAllExclusionResponder.class);

  public DisconnectAllExclusionResponder(ConnectionController controller) {
    this.connectionController = controller;
  }

  @Override
  public void onExclusion() {
    log.warn("SelfExclusionResponder detected exclusion");
    connectionController.disconnectAll("onExclusion");
  }
}
