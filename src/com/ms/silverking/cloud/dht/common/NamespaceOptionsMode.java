package com.ms.silverking.cloud.dht.common;

/**
 * Controls if NamespaceOptions shall be stored in MetaNamespace or ZooKeeper
 */
public enum NamespaceOptionsMode {
  /**
   * Use NSPImpl which stores namespaceOptions into __DHT_Meta__ namespace and "properties" file
   */
  MetaNamespace,
  /**
   * Use ZooKeeperImpl which stores full namespaceOptions into ZooKeeper node
   */
  ZooKeeper
}