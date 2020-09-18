package com.ms.silverking.cloud.dht.client.impl;

import com.ms.silverking.id.UUIDBase;

public interface ActiveOperationListeners {
  boolean isResponsibleFor(UUIDBase messageId);
}
