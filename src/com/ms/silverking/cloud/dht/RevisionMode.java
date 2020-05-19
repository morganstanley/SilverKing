package com.ms.silverking.cloud.dht;

/**
 * Mode of revisions supported by a namespace. A revision is the creation of a value with a version &lt;= the
 * latest stored version (creation time is always monotonically increasing.)
 */
public enum RevisionMode {
  /**
   * No revisions allowed
   */
  NO_REVISIONS,
  /**
   * Unrestricted revisions allowed
   */
  UNRESTRICTED_REVISIONS
}
