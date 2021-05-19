package com.ms.silverking.cloud.dht.common;

import java.io.File;
import java.io.IOException;

import com.ms.silverking.cloud.dht.client.ClientDHTConfigurationProvider;
import com.ms.silverking.cloud.dht.daemon.storage.NamespacePropertiesIO;

public class NamespaceOptionsClientPropertiesImpl extends NamespaceOptionsClientBase {
  private final static String implName = "Properties";

  public NamespaceOptionsClientPropertiesImpl(ClientDHTConfigurationProvider dhtConfigProvider) {
    super(dhtConfigProvider);
  }

  @Override
  protected long getDefaultRelTimeoutMillis() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void putNamespaceProperties(long nsContext, NamespaceProperties nsProperties)
      throws NamespacePropertiesPutException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void deleteNamespaceProperties(long nsContext) throws NamespacePropertiesDeleteException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected String implementationName() {
    return implName;
  }

  @Override
  public NamespaceProperties getNsPropertiesForRecovery(File nsDir) throws NamespacePropertiesRetrievalException {
    try {
      // This implementation will still need "properties" file for bootstrap in recovery
      return NamespacePropertiesIO.read(nsDir);
    } catch (IOException ioe) {
      throw new NamespacePropertiesRetrievalException(ioe);
    }
  }

  @Override
  protected NamespaceProperties retrieveFullNamespaceProperties(long nsContext)
      throws NamespacePropertiesRetrievalException {
    throw new UnsupportedOperationException();
  }
}
