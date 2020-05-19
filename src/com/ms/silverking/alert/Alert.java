package com.ms.silverking.alert;

import com.google.common.base.Preconditions;

public class Alert {
  private final String context;
  private final int level;
  private final String key;
  private final String message;
  private final String data;

  public Alert(String context, int level, String key, String message, String data) {
    Preconditions.checkNotNull(context);
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(message);
    this.context = context;
    this.level = level;
    this.key = key;
    this.message = message;
    this.data = data != null ? data : "";
  }

  public String getContext() {
    return context;
  }

  public int getLevel() {
    return level;
  }

  public String getKey() {
    return key;
  }

  public String getMessage() {
    return message;
  }

  public String getData() {
    return data;
  }

  @Override
  public int hashCode() {
    return context.hashCode() ^ Integer.hashCode(level) ^ key.hashCode() ^ message.hashCode() ^ data.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    Alert o;

    o = (Alert) obj;
    return context.equals(o.context) && level == o.level && key.equals(o.key) && message.equals(
        o.message) && data.equals(o.data);
  }

  @Override
  public String toString() {
    return context + ":" + level + ":" + key + ":" + message;
  }
}
