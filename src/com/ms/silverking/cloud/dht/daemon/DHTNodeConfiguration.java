package com.ms.silverking.cloud.dht.daemon;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.log.Log;
import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.PropertiesHelper.UndefinedAction;

/**
 * Provide DHTNode specific configuration information. This is not used to specify configuration
 * information, but rather to read in configuration information that is provided elsewhere.
 */
public class DHTNodeConfiguration {
  private static final String dataBasePathProperty = DHTNodeConfiguration.class.getPackage().getName() +
      ".DataBasePath";
  private static final String dataBasePathPropertyValue = PropertiesHelper.systemHelper.getString(dataBasePathProperty,
      UndefinedAction.ZeroOnUndefined);
  // The below default path should not be used in a properly functioning system as the management
  // infrastructure should be setting the property on launch.
  private String dataBasePath; // The actual path in use at this node DHTConstants dataBasePath provides a list of
  // possible paths

  private enum InvalidPathAction {Warn, ThrowException}

  ;

  public DHTNodeConfiguration() {
    if (dataBasePathPropertyValue != null && dataBasePathPropertyValue.trim().length() != 0) {
      Log.warning("Found dataBasePathProperty " + dataBasePathPropertyValue);
      setDataBasePath(dataBasePathPropertyValue.trim());
    } else {
      // Warn since a properly functioning system should have specified the property.
      Log.warning("No dataBasePathProperty specified. Using default.");
      setDataBasePath(DHTConstants.defaultDataBasePath, InvalidPathAction.Warn);
    }
  }

  public String getDataBasePath() {
    if (dataBasePath == null) {
      throw new RuntimeException("No valid dataBasePath specified");
    } else {
      return dataBasePath;
    }
  }

  public DHTNodeConfiguration(String dataBasePath) {
    setDataBasePath(dataBasePath);
  }

  private void setDataBasePath(String __dataBasePath) {
    setDataBasePath(__dataBasePath, InvalidPathAction.ThrowException);
  }

  private void setDataBasePath(String __dataBasePath, InvalidPathAction invalidPathAction) {
    String[] candidatePaths;
    String _dataBasePath;

    _dataBasePath = null;
    candidatePaths = __dataBasePath.split(DHTConstants.dataBasePathDelimiter);
    if (candidatePaths.length == 0) {
      throw new RuntimeException("No candidate paths. Empty " + dataBasePathProperty + "?");
    } else {
      String existingPath;
      int numExistingPaths;

      // First check to see if more than one candidate path exists
      // We error out on this condition as this is a risky practice.
      // Moving from one path to another can cause data to disappear.
      // We expect users to ensure that only one path exists.
      // We verify that here so that we don't have data silently disappear.

      existingPath = null;
      numExistingPaths = 0;
      for (String candidatePath : candidatePaths) {
        if (Files.isWritable(Paths.get(candidatePath))) {
          ++numExistingPaths;
          existingPath = candidatePath;
          Log.warningf("Found candidate dataBase path %s", candidatePath);
        }
      }
      if (numExistingPaths > 1) {
        throw new RuntimeException("More than one candidate dataBase path exists in " + __dataBasePath);
      } else if (numExistingPaths == 1) {
        _dataBasePath = existingPath;
      } else {
        // In this case, no existing paths have been found.
        // We attempt creation in the order specified.
        // We use the first successfully created path.
        for (String candidatePath : candidatePaths) {
          File f;

          f = new File(candidatePath);
          f.mkdirs();
          if (Files.isWritable(f.toPath())) {
            _dataBasePath = candidatePath;
            break;
          }
        }
      }
    }
    if (_dataBasePath != null) {
      this.dataBasePath = _dataBasePath;
      Log.warning("dataBasePath: ", this.dataBasePath);
    } else {
      switch (invalidPathAction) {
      case Warn:
        Log.warning("Can't find valid dataBasePath from candidates: " + __dataBasePath);
        Log.warning(
            "Either DHTNodeConfiguration.setDataBasePath() must be called later, or the candidates must be corrected");
        break;
      case ThrowException:
        throw new RuntimeException("Can't find valid dataBasePath from candidates: " + __dataBasePath);
      default:
        throw new RuntimeException("Panic");
      }
    }
  }
}
