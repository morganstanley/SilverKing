package com.ms.silverking.cloud.dht.client.impl;

import com.ms.silverking.cloud.dht.NamespaceCreationOptions;
import com.ms.silverking.cloud.dht.client.NamespaceCreationException;
import com.ms.silverking.cloud.dht.client.NamespaceModificationException;
import com.ms.silverking.cloud.dht.common.NamespaceProperties;
import com.ms.silverking.cloud.dht.common.NamespacePropertiesDeleteException;
import com.ms.silverking.cloud.dht.common.NamespacePropertiesRetrievalException;
import com.ms.silverking.cloud.dht.common.TimeoutException;

public interface NamespaceOptionsClientCS {
  ////// ====== client side namespace admin API (take human-readable namespace name as arg) ======
  void createNamespace(String nsName, NamespaceProperties nsProperties) throws NamespaceCreationException;

  void modifyNamespace(String nsName, NamespaceProperties nsProperties) throws NamespaceModificationException;

  void deleteNamespace(String nsName) throws NamespacePropertiesDeleteException;

  ////// ====== client side internal query API (take human-readable namespace name as arg) ======
  NamespaceProperties getNamespacePropertiesAndTryAutoCreate(String nsName)
      throws NamespacePropertiesRetrievalException, TimeoutException;

  NamespaceCreationOptions getNamespaceCreationOptions();
}
