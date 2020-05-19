package com.ms.silverking.cloud.dht.common;

import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.log.Log;
import com.ms.silverking.text.ObjectDefParser2;

/**
 * Internal metadata for a Namespace.
 */
public class NamespaceProperties {
  private final NamespaceOptions options;
  private final String name;   // optional field
  private final String parent;
  private final long minVersion;
  private final long creationTime; // optional field

  private static final NamespaceProperties templateProperties = new NamespaceProperties();
  private static final Set<String> optionalFields = ImmutableSet.of("name", "creationTime");
  // For backward compatibility only (we drop "creationTime" and "name", since its not in the old data)
  private static final Set<String> legacyExclusionFields = ImmutableSet.of("creationTime", "name");

  private static final long noCreationTime = 0;

  static {
    ObjectDefParser2.addParserWithOptionalFields(templateProperties, optionalFields);
  }

  public static void initParser() {
  }

  public NamespaceProperties(NamespaceOptions options, String name, String parent, long minVersion, long creationTime) {
    assert options != null;
    this.options = options;
    this.name = name;
    this.parent = parent;
    this.minVersion = minVersion;
    this.creationTime = creationTime;
  }

  public NamespaceProperties(NamespaceOptions options, String name, String parent, long minVersion) {
    this(options, name, parent, minVersion, noCreationTime);
  }

  public NamespaceProperties(NamespaceOptions options, String parent, long minVersion) {
    /* For backward compatibility, if no name is specified, null is used as placeholder
     * NOTE:
     *  - new version of code will always enrich nsProperties with name
     *  - old version of code will leave name as null
     */
    this(options, null, parent, minVersion);
  }

  public NamespaceProperties(NamespaceOptions options) {
    this(options, null, Long.MIN_VALUE);
  }

  private NamespaceProperties() {
    this(DHTConstants.defaultNamespaceOptions);
  }

  public NamespaceOptions getOptions() {
    return options;
  }

  public String getParent() {
    return parent;
  }

  public long getMinVersion() {
    return minVersion;
  }

  public String getName() {
    return name;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public boolean hasCreationTime() {
    return creationTime != noCreationTime;
  }

  public boolean hasName() {
    return name != null;
  }

  public NamespaceProperties creationTime(long creationTime) {
    return new NamespaceProperties(options, name, parent, minVersion, creationTime);
  }

  public NamespaceProperties options(NamespaceOptions options) {
    return new NamespaceProperties(options, name, parent, minVersion, creationTime);
  }

  public NamespaceProperties name(String name) {
    return new NamespaceProperties(options, name, parent, minVersion, creationTime);
  }

  @Override
  public int hashCode() {
    return options.hashCode() ^ (parent == null ? 0 : parent.hashCode()) ^ (hasName() ? name.hashCode() : 0) ^ (
        hasCreationTime() ?
            Long.hashCode(creationTime) :
            0);
  }

  // Used in server side, since server has full nsProperties
  @Override
  public boolean equals(Object o) {
    NamespaceProperties oProperties;

    oProperties = (NamespaceProperties) o;
    return this.options.equals(oProperties.options) && Objects.equals(this.parent,
        oProperties.parent) && Objects.equals(this.name,
        oProperties.name) && this.creationTime == oProperties.creationTime;
  }

  // Used in client side nsCreation creation/update, since client only has partial nsProperties
  public boolean partialEquals(NamespaceProperties other) {
    // 1. Drop creationTime, since its issued in serverside store (client doesn't have)
    // 2. Conditionally check name, new version silverking ensures client will always enrich nsProperties with name;
    //    => If at least one NamespaceProperties has no name, then don't check
    boolean needToCheckName;

    needToCheckName = this.hasName() && other.hasName();
    return this.options.equals(other.options) && Objects.equals(this.parent,
        other.parent) && (!needToCheckName || Objects.equals(this.name, other.name));
  }

  public boolean canBeReplacedBy(NamespaceProperties other) {
    if (this == other) {
      return true;
    }

    return NamespaceUtil.canMutateWith(options, other.options) && Objects.equals(this.parent,
        other.parent) && Objects.equals(this.name, other.name);
  }

  public void debugEquals(Object o) {
    NamespaceProperties oProperties;

    oProperties = (NamespaceProperties) o;
    this.options.debugEquality(oProperties.options);
  }

  // For backward compatibility only
  public String toLegacySKDef() {
    //  We drop "creationTime" and "name", since its not in the old data
    return ObjectDefParser2.objectToStringWithExclusions(this, legacyExclusionFields);
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }

  /**
   * @param def NamespaceProperties reflection string
   * @return NamespaceProperties object whose creationTime needs to be checked
   */
  public static NamespaceProperties parse(String def) {
    // Caller needs to check creation of parsed result
    return ObjectDefParser2.parse(NamespaceProperties.class, def);
  }

  /**
   * @param def          NamespaceProperties reflection string, the NamespaceProperties is assumed to has NO
   *                     creation time
   * @param creationTime the creation time to assign to this NamespaceProperties
   * @return NamespaceProperties object
   */
  public static NamespaceProperties parse(String def, long creationTime) {
    NamespaceProperties nsProperties = ObjectDefParser2.parse(NamespaceProperties.class, def);
    if (nsProperties.hasCreationTime()) {
      Log.warning(
          "The parsed NamespaceProperties already has creationTime [" + nsProperties.getCreationTime() + "] when " +
              "trying to parse it with creationTime [" + creationTime + "]");
    }
    return nsProperties.creationTime(creationTime);
  }
}
