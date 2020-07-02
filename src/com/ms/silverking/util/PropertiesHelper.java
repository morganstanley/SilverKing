package com.ms.silverking.util;

import java.io.PrintStream;
import java.text.ParseException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import com.ms.silverking.log.Log;
import com.ms.silverking.text.StringUtil;

/**
 * Simplifies common interaction with java.util.Properties
 */
public class PropertiesHelper {
  private final Properties prop;
  private final LogMode logMode;

  public enum LogMode {Silent, Exceptions, UndefinedAndExceptions}

  ;

  public enum ParseExceptionAction {DefaultOnParseException, RethrowParseException}

  ;

  public enum UndefinedAction {DefaultOnUndefined, ExceptionOnUndefined, ZeroOnUndefined}

  ;

  public static final PropertiesHelper systemHelper;
  public static final PropertiesHelper envHelper;

  private static final ParseExceptionAction standardParseExceptionAction = ParseExceptionAction.DefaultOnParseException;
  private static final UndefinedAction standardUndefinedAction = UndefinedAction.ExceptionOnUndefined;
  private static final LogMode standardLogMode = LogMode.Silent;

  static {
    Properties envProperties;

    envProperties = new Properties();
    for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
      envProperties.setProperty(entry.getKey(), entry.getValue());
    }
    envHelper = new PropertiesHelper(envProperties);
    systemHelper = new PropertiesHelper(System.getProperties());
  }

  public PropertiesHelper(Properties prop, LogMode logMode) {
    this.prop = prop;
    this.logMode = logMode;
  }

  public PropertiesHelper(Properties prop) {
    this(prop, standardLogMode);
  }

  public PropertiesHelper() {
    this(new Properties(), standardLogMode);
  }

  private void verifyNotDefaultOnUndefined(UndefinedAction undefinedAction) {
    if (undefinedAction == UndefinedAction.DefaultOnUndefined) {
      throw new RuntimeException("Wrong method for DefaultOnUndefined");
    }
  }

  private void logZeroOnUndefined(String name) {
    if (logMode == LogMode.UndefinedAndExceptions) {
      Log.warning("Property undefined, using zero/null: " + name);
    }
  }

  private void logDefaultOnUndefined(String name, Object defaultValue) {
    if (logMode == LogMode.UndefinedAndExceptions) {
      Log.warning("Property undefined, using default: " + name + " = " + defaultValue);
    }
  }

  private void throwExceptionOnUndefined(String name) {
    if (logMode != LogMode.Silent) {
      Log.warning("Property undefined: " + name);
    }
    throw new PropertyException("Required property undefined: " + name);
  }

  private void logDefaultOnParseException(String name, Object defaultValue) {
    if (logMode != LogMode.Silent) {
      Log.warning("Property can't be parsed, using default: " + name + " = " + defaultValue);
    }
  }

  // String

  String getString(String name, String defaultValue, UndefinedAction undefinedAction) {
    String def;

    def = prop.getProperty(name);
    if (def != null) {
      return def;
    } else {
      return getUndefinedValue(name, undefinedAction, null, defaultValue);
    }
  }

  private <T> T getUndefinedValue(String name, UndefinedAction undefinedAction, T zeroValue, T defaultValue) {
    switch (undefinedAction) {
    case ZeroOnUndefined:
      logZeroOnUndefined(name);
      return zeroValue;
    case DefaultOnUndefined:
      logDefaultOnUndefined(name, defaultValue);
      return defaultValue;
    case ExceptionOnUndefined:
      throwExceptionOnUndefined(name);
    default:
      throw new RuntimeException("panic");
    }
  }

  public String getString(String name, String defaultValue) {
    return getString(name, defaultValue, UndefinedAction.DefaultOnUndefined);
  }

  public String getString(String name, UndefinedAction undefinedAction) {
    verifyNotDefaultOnUndefined(undefinedAction);
    return getString(name, null, undefinedAction);
  }

  public String getString(String name) {
    return getString(name, standardUndefinedAction);
  }

  // int

  int getInt(String name, int defaultValue, UndefinedAction undefinedAction,
      ParseExceptionAction parseExceptionAction) {
    String def;

    def = prop.getProperty(name);
    if (def != null) {
      try {
        return Integer.parseInt(def);
      } catch (NumberFormatException nfe) {
        return getExceptionValue(name, parseExceptionAction, defaultValue, nfe);
      }
    } else {
      return getUndefinedValue(name, undefinedAction, 0, defaultValue);
    }
  }

  private <T> T getExceptionValue(String name, ParseExceptionAction parseExceptionAction, T defaultValue,
      RuntimeException exceptionToThrow) {
    switch (parseExceptionAction) {
    case DefaultOnParseException:
      logDefaultOnParseException(name, defaultValue);
      return defaultValue;
    default:
      throw exceptionToThrow;
    }
  }

  public int getInt(String name, int defaultValue, ParseExceptionAction parseExceptionAction) {
    return getInt(name, defaultValue, UndefinedAction.DefaultOnUndefined, parseExceptionAction);
  }

  public int getInt(String name, int defaultValue) {
    return getInt(name, defaultValue, UndefinedAction.DefaultOnUndefined, ParseExceptionAction.DefaultOnParseException);
  }

  public int getInt(String name, UndefinedAction undefinedAction, ParseExceptionAction parseExceptionAction) {
    verifyNotDefaultOnUndefined(undefinedAction);
    return getInt(name, 0, undefinedAction, parseExceptionAction);
  }

  public int getInt(String name, UndefinedAction undefinedAction) {
    verifyNotDefaultOnUndefined(undefinedAction);
    return getInt(name, 0, undefinedAction, standardParseExceptionAction);
  }

  public int getInt(String name, ParseExceptionAction parseExceptionAction) {
    return getInt(name, 0, standardUndefinedAction, parseExceptionAction);
  }

  public int getInt(String name) {
    return getInt(name, 0, standardUndefinedAction, standardParseExceptionAction);
  }

  // boolean

  boolean getBoolean(String name, boolean defaultValue, UndefinedAction undefinedAction,
      ParseExceptionAction parseExceptionAction) {
    String def;

    def = prop.getProperty(name);
    if (def != null) {
      try {
        return StringUtil.parseBoolean(def);
      } catch (ParseException pe) {
        return getExceptionValue(name, parseExceptionAction, defaultValue, new RuntimeException(pe));
      }
    } else {
      return getUndefinedValue(name, undefinedAction, false, defaultValue);
    }
  }

  public boolean getBoolean(String name, boolean defaultValue, ParseExceptionAction parseExceptionAction) {
    return getBoolean(name, defaultValue, UndefinedAction.DefaultOnUndefined, parseExceptionAction);
  }

  public boolean getBoolean(String name, UndefinedAction undefinedAction, ParseExceptionAction parseExceptionAction) {
    verifyNotDefaultOnUndefined(undefinedAction);
    return getBoolean(name, false, undefinedAction, parseExceptionAction);
  }

  public boolean getBoolean(String name, UndefinedAction undefinedAction) {
    verifyNotDefaultOnUndefined(undefinedAction);
    return getBoolean(name, false, undefinedAction, standardParseExceptionAction);
  }

  public boolean getBoolean(String name, ParseExceptionAction parseExceptionAction) {
    return getBoolean(name, false, standardUndefinedAction, parseExceptionAction);
  }

  public boolean getBoolean(String name, boolean defaultValue) {
    return getBoolean(name, defaultValue, UndefinedAction.DefaultOnUndefined, standardParseExceptionAction);
  }

  public boolean getBoolean(String name) {
    return getBoolean(name, false, standardUndefinedAction, standardParseExceptionAction);
  }

  // long

  long getLong(String name, long defaultValue, UndefinedAction undefinedAction,
      ParseExceptionAction parseExceptionAction) {
    String def;

    def = prop.getProperty(name);
    if (def != null) {
      try {
        return Long.parseLong(def);
      } catch (NumberFormatException nfe) {
        return getExceptionValue(name, parseExceptionAction, defaultValue, nfe);
      }
    } else {
      return getUndefinedValue(name, undefinedAction, 0L, defaultValue);
    }
  }

  public long getLong(String name, long defaultValue, ParseExceptionAction parseExceptionAction) {
    return getLong(name, defaultValue, UndefinedAction.DefaultOnUndefined, parseExceptionAction);
  }

  public long getLong(String name, UndefinedAction undefinedAction, ParseExceptionAction parseExceptionAction) {
    verifyNotDefaultOnUndefined(undefinedAction);
    return getLong(name, 0, undefinedAction, parseExceptionAction);
  }

  public long getLong(String name, UndefinedAction undefinedAction) {
    verifyNotDefaultOnUndefined(undefinedAction);
    return getLong(name, 0, undefinedAction, standardParseExceptionAction);
  }

  public long getLong(String name, ParseExceptionAction parseExceptionAction) {
    return getLong(name, 0, standardUndefinedAction, parseExceptionAction);
  }

  public long getLong(String name, long defaultValue) {
    return getLong(name, defaultValue, UndefinedAction.DefaultOnUndefined,
        ParseExceptionAction.DefaultOnParseException);
  }

  public long getLong(String name) {
    return getLong(name, 0, standardUndefinedAction, standardParseExceptionAction);
  }

  // double
  
  double getDouble(String name, double defaultValue, UndefinedAction undefinedAction,
      ParseExceptionAction parseExceptionAction) {
    String def;

    def = prop.getProperty(name);
    if (def != null) {
      try {
        return Double.parseDouble(def);
      } catch (NumberFormatException nfe) {
        return getExceptionValue(name, parseExceptionAction, defaultValue, nfe);
      }
    } else {
      return getUndefinedValue(name, undefinedAction, 0.0, defaultValue);
    }
  }

  public double getDouble(String name, double defaultValue) {
    return getDouble(name, defaultValue, UndefinedAction.DefaultOnUndefined,
        ParseExceptionAction.DefaultOnParseException);
  }

  // Enum

  public <T extends Enum> T getEnum(String name, T defaultValue, UndefinedAction undefinedAction,
      ParseExceptionAction parseExceptionAction) {
    if (defaultValue == null) {
      throw new RuntimeException("defaultValue cannot be null for enum");
    } else {
    String def;

    def = prop.getProperty(name);
    if (def != null) {
      try {
          return (T) defaultValue.valueOf(defaultValue.getClass(), def);
        } catch (IllegalArgumentException iae) {
          return getExceptionValue(name, parseExceptionAction, defaultValue, iae);
      }
    } else {
        return (T) getUndefinedValue(name,
            undefinedAction != null ? undefinedAction : UndefinedAction.DefaultOnUndefined, null, defaultValue);
      }
    }
  }

  public <T extends Enum> T getEnum(String name, T defaultValue, ParseExceptionAction parseExceptionAction) {
    return getEnum(name, defaultValue, UndefinedAction.DefaultOnUndefined, parseExceptionAction);
    }

  public <T extends Enum> T getEnum(String name, T defaultValue, UndefinedAction undefinedAction) {
    verifyNotDefaultOnUndefined(undefinedAction);
    return getEnum(name, defaultValue, undefinedAction, standardParseExceptionAction);
  }

  public <T extends Enum> T getEnum(String name, T defaultValue) {
    return getEnum(name, defaultValue, UndefinedAction.DefaultOnUndefined, standardParseExceptionAction);
  }

  ///////////////////////
  public void displayProperties() {
    displayProperties(System.out);
  }

  public void displayProperties(PrintStream out) {
    for (Entry<Object, Object> entry : prop.entrySet()) {
      out.printf("%s\t%s\n", entry.getKey(), entry.getValue());
    }
  }
}
