package com.ms.silverking.cloud.dht.client.crypto;

import static com.ms.silverking.cloud.dht.client.crypto.EncrypterDecrypter.keyFilePropertyName;

import java.io.File;
import java.io.IOException;

import com.ms.silverking.io.FileUtil;
import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.PropertiesHelper.UndefinedAction;

public class Util {

  public static byte[] getBytesFromKeyFile() throws IOException {
    return FileUtil.readFileAsBytes(
        new File(PropertiesHelper.systemHelper.getString(keyFilePropertyName, UndefinedAction.ExceptionOnUndefined)));
  }

}
