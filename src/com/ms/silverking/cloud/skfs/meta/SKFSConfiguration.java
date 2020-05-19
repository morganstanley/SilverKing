package com.ms.silverking.cloud.skfs.meta;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

import com.ms.silverking.cloud.meta.VersionedDefinition;

/**
 * SKFS configuration settings.
 */
public class SKFSConfiguration implements VersionedDefinition {
  private final List<String> vars;
  private final String config;
  private final long version;
  private final long zxid;

  public SKFSConfiguration(String configName, List<String> vars, long version, long zxid) {
    this.config = configName;
    this.version = version;
    this.zxid = zxid;
    this.vars = vars;
  }

  public SKFSConfiguration version(long version) {
    return new SKFSConfiguration(config, vars, version, zxid);
  }

  public SKFSConfiguration zkid(long zkid) {
    return new SKFSConfiguration(config, vars, version, zkid);
  }

  public String getConfig() {
    return config;
  }

  @Override
  public long getVersion() {
    return version;
  }

  public long getZKID() {
    return zxid;
  }

  public static SKFSConfiguration parse(String skfsConfName, String def, long version) {
    try {
      return parse(skfsConfName, new ByteArrayInputStream(def.getBytes()), version);
    } catch (IOException ioe) {
      throw new RuntimeException("Unexpected exception", ioe);
    }
  }

  public static SKFSConfiguration parse(String skfsConfName, InputStream inStream, long version) throws IOException {
    try {
      List<String> strList = new LinkedList<>();
      String line;
      BufferedReader reader = new BufferedReader(new InputStreamReader(inStream));
      do {
        line = reader.readLine();
        strList.add(line);

      } while (line != null);
      return new SKFSConfiguration(skfsConfName, strList, version, 0);
    } finally {
      inStream.close();
    }
  }

  @Override
  public String toString() {
    StringBuilder sb;
    sb = new StringBuilder();
    for (String var : vars) {
      sb.append(var);
      sb.append('\n');
    }
    return sb.toString();
  }

}
