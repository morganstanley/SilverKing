package com.ms.silverking.cloud.dht.meta;

import com.ms.silverking.cloud.meta.VersionedDefinition;
import com.ms.silverking.io.FileUtil;
import com.ms.silverking.text.FieldsRequirement;
import com.ms.silverking.text.ObjectDefParser2;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class IpAliasConfiguration implements VersionedDefinition {

  private final long version;
  private final Map<String, String> ipAliasMap;

  public static final IpAliasConfiguration emptyTemplate = new IpAliasConfiguration(0, null);

  public IpAliasConfiguration(long version, Map<String, String> ipAliasMap) {
    this.version = version;
    this.ipAliasMap = ipAliasMap;
  }

  static {
    ObjectDefParser2.addParser(emptyTemplate, FieldsRequirement.ALLOW_INCOMPLETE);
  }

  public static IpAliasConfiguration parse(String def, long version) {
    IpAliasConfiguration instance;
    instance = ObjectDefParser2.parse(IpAliasConfiguration.class, def);
    return instance.version(version);
  }

  public static IpAliasConfiguration readFromFile(String fileName) throws IOException {
    return readFromFile(new File(fileName));
  }

  public static IpAliasConfiguration readFromFile(File f) throws IOException {
    String  def;

    def = FileUtil.readFileAsString(f);
    return parse(def, 0);
  }

  public IpAliasConfiguration version(long version) {
    return new IpAliasConfiguration(version, this.ipAliasMap);
  }

  public IpAliasConfiguration ipAliasMap(Map<String, String> ipAliasMap) {
    return new IpAliasConfiguration(this.version, ipAliasMap);
  }

  @Override
  public long getVersion() {
    return version;
  }

  public Map<String, String> getIPAliasMap() {
    return ipAliasMap;
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }
}
