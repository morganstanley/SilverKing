package com.ms.silverking.cloud.dht.common;

import com.ms.silverking.cloud.dht.NamespaceCreationOptions;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.client.NamespaceCreationException;
import com.ms.silverking.cloud.dht.client.NamespaceModificationException;

import java.io.File;

public interface NamespaceOptionsClient {
    ////// ====== Client side namespace admin API (take human-readable namespace name as arg) ======
    void createNamespace(String nsName, NamespaceProperties nsProperties) throws NamespaceCreationException;
    void modifyNamespace(String nsName, NamespaceProperties nsProperties) throws NamespaceModificationException;
    void deleteNamespace(String nsName) throws NamespacePropertiesDeleteException;

    ////// ====== Server side internal query API (take namespace context as arg) ======
    NamespaceProperties getNamespaceProperties(long nsContext) throws NamespacePropertiesRetrievalException;
    NamespaceOptions getNamespaceOptions(long nsContext) throws NamespacePropertiesRetrievalException;
    NamespaceProperties getNamespacePropertiesWithTimeout(long nsContext, long relTimeoutMillis) throws NamespacePropertiesRetrievalException, TimeoutException;
    // For backward compatibility, since some implementation may still need properties file to bootstrap
    NamespaceProperties getNsPropertiesForRecovery(File nsDir) throws NamespacePropertiesRetrievalException;

    ////// ====== Client side internal query API (take human-readable namespace name as arg) ======
    NamespaceProperties getNamespacePropertiesAndTryAutoCreate(String nsName) throws NamespacePropertiesRetrievalException, TimeoutException;
    NamespaceCreationOptions getNamespaceCreationOptions();
}
