package com.ms.silverking.cloud.dht;

/**
 * Mode of versioning supported by a namespace.
 */
public enum NamespaceVersionMode {
  /**
   * Only one version may exist. Version numbers are undefined and should not be used externally.
   */
  SINGLE_VERSION,
  /**
   * Multiple versions of a value may exist. The client must explicitly specify the version.
   */
  CLIENT_SPECIFIED,
  /**
   * Versions will be generated automatically from the positive integers
   */
  SEQUENTIAL,
  /**
   * Versions will be generated automatically using the system time in milliseconds
   */
  SYSTEM_TIME_MILLIS,
  /**
   * Versions will be generated automatically using the time in nanoseconds. This time will
   * be drawn from the same time source as creation times.
   */
  SYSTEM_TIME_NANOS;

  /**
   * Determine if a version is supported by this mode
   *
   * @param version the version
   * @return true if the given version is supported by this mode
   */
  public boolean validVersion(long version) {
        /*
        switch (this) {
        case SINGLE_VERSION: return version == DHTConstants.writeOnceVersion;
        default: return true;
        }
        commented out to switch to version SINGLE_VERSION implementation that 
        uses versions internally so that ns clones can leverage versioned
        implementation without requiring special cases
        */
    return true;
  }

  /**
   * Determine if this mode is specified by the system
   *
   * @return true iff the given mode is specified by the system
   */
  public boolean isSystemSpecified() {
    switch (this) {
    case SINGLE_VERSION:
      return false;
    case CLIENT_SPECIFIED:
      return false;
    case SEQUENTIAL:
      return true;
    case SYSTEM_TIME_MILLIS:
      return true;
    case SYSTEM_TIME_NANOS:
      return true;
    default:
      throw new RuntimeException("panic");
    }
  }
}
