package com.ms.silverking;

import com.ms.silverking.util.PropertiesHelper;

public class SKTestConfiguration {
  public static final String zkEnsembleProperty = SKTestConfiguration.class.getCanonicalName() + ".ZKEnsemble";
  public static final String zkEnsemble;

  static {
    zkEnsemble = PropertiesHelper.systemHelper.getString(zkEnsembleProperty,
        PropertiesHelper.UndefinedAction.ZeroOnUndefined);
    //PropertiesHelper.UndefinedAction.ExceptionOnUndefined);
    // FIXME - add to config and throw on undefined
  }
}
