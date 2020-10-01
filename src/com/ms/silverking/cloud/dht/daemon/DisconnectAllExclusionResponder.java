package com.ms.silverking.cloud.dht.daemon;

import com.ms.silverking.log.Log;
import com.ms.silverking.net.async.ConnectionController;

public class DisconnectAllExclusionResponder implements SelfExclusionResponder {

  ConnectionController connectionController;

  public DisconnectAllExclusionResponder(ConnectionController controller) {
    this.connectionController = controller;
  }

  @Override
  public void onExclusion() {
    Log.warning("SelfExclusionResponder detected exclusion");
    connectionController.disconnectAll("onExclusion");
  }
}
