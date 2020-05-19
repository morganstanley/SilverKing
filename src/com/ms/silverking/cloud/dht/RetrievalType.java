package com.ms.silverking.cloud.dht;

/**
 * Specifies the type of retrieval operation to perform. Types that do
 * not specify value retrieval are guaranteed to not retrieve the value.
 * The EXISTENCE operation returns StoredValues that are non-null if the
 * given mapping exists and null otherwise.
 * All retrievals may optionally fetch meta data whether
 * or not it is specified, but this not guaranteed to exist.
 */
public enum RetrievalType {
  /**
   * Return values
   */
  VALUE,
  /**
   * Return value meta data
   */
  META_DATA,
  /**
   * Return both values and meta data
   */
  VALUE_AND_META_DATA,
  /**
   * Only query existence. Values will not be returned.
   */
  EXISTENCE;

  /**
   * True iff this type is guaranteed to result in value retrieval.
   *
   * @return true iff this type is guaranteed to result in value retrieval.
   */
  public boolean hasValue() {
    switch (this) {
    case VALUE:
      return true;
    case META_DATA:
      return false;
    case VALUE_AND_META_DATA:
      return true;
    case EXISTENCE:
      return false;
    default:
      throw new RuntimeException("panic");
    }
  }

  /**
   * True iff this type is guaranteed to result in meta data retrieval.
   *
   * @return true iff this type is guaranteed to result in meta data retrieval
   */
  public boolean hasMetaData() {
    switch (this) {
    case VALUE:
      return false;
    case META_DATA:
      return true;
    case VALUE_AND_META_DATA:
      return true;
    case EXISTENCE:
      return false;
    default:
      throw new RuntimeException("panic");
    }
  }
}
