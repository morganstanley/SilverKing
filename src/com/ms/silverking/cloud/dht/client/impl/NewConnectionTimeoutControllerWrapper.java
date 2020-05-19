package com.ms.silverking.cloud.dht.client.impl;

import com.ms.silverking.cloud.dht.client.SessionEstablishmentTimeoutController;
import com.ms.silverking.net.AddrAndPort;
import com.ms.silverking.net.async.NewConnectionTimeoutController;

/**
 * Allows a SessionEstablishmentTimeoutController to be used as the basis of a NewConnectionTimeoutController
 */
class NewConnectionTimeoutControllerWrapper implements NewConnectionTimeoutController {
  private final SessionEstablishmentTimeoutController sessionEstablishmentTimeoutController;

  NewConnectionTimeoutControllerWrapper(SessionEstablishmentTimeoutController sessionEstablishmentTimeoutController) {
    this.sessionEstablishmentTimeoutController = sessionEstablishmentTimeoutController;
  }

  @Override
  public int getMaxAttempts(AddrAndPort addrAndPort) {
    return sessionEstablishmentTimeoutController.getMaxAttempts(null);
  }

  @Override
  public long getRelativeTimeoutMillisForAttempt(AddrAndPort addrAndPort, int attemptIndex) {
    return sessionEstablishmentTimeoutController.getRelativeTimeoutMillisForAttempt(null, attemptIndex);
  }

  @Override
  public long getMaxRelativeTimeoutMillis(AddrAndPort addrAndPort) {
    return sessionEstablishmentTimeoutController.getMaxRelativeTimeoutMillis(null);
  }
}
