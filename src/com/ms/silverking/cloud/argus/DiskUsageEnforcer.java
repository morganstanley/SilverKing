package com.ms.silverking.cloud.argus;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ms.silverking.log.Log;
import com.ms.silverking.os.linux.proc.ProcReader;
import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.PropertiesHelper.ParseExceptionAction;

/**
 * Enforces limits on process disk usage.
 */
public class DiskUsageEnforcer implements SafetyEnforcer {

  private static final long defaultDiskUsageLimitMB = (long) 10 * 1024;
  private static final long ONEMB = 1024 * 1024;
  //private static final long   diskUsageLimit = 10 * 1024 * 1024;
  //private static final long   diskUsageLimit = 1024;
  private static final int defaultIntervalMillis = 30 * 1000;
  private static final String defaultPaths = "/tmp:/var/tmp";
  private static final String defaultPropExceptions = ".*java.*:.*Argus.*";
  private static final String propDiskUsageLimit = "diskUsageLimitMB";
  private static final String propIntervalMillis = "intervalMillis";
  private static final String propPaths = "paths";
  private static final String propExceptions = "DiskUsageExceptions";
  private static final String delimiter = ":";
  private final ProcReader procReader;
  private final Map<Integer, ProcessDiskUsage> diskUsageMap;
  private final List<String> exceptions;
  private final Terminator terminator;
  private final long diskUsageLimit;
  private final int intervalMillis;
  private final String[] paths;

  public DiskUsageEnforcer(PropertiesHelper ph, Terminator terminator) {

    this.terminator = terminator;
    procReader = new ProcReader();
    diskUsageMap = new HashMap<>();

    diskUsageLimit = ph.getLong(propDiskUsageLimit, defaultDiskUsageLimitMB,
        ParseExceptionAction.DefaultOnParseException) * ONEMB;
    intervalMillis = ph.getInt(propIntervalMillis, defaultIntervalMillis, ParseExceptionAction.DefaultOnParseException);
    paths = ph.getString(propPaths, defaultPaths).split(delimiter);
    String[] exs = ph.getString(propExceptions, defaultPropExceptions).split(delimiter);
    exceptions = Arrays.asList(exs);

  }

  @Override
  public int enforce() {
    List<Integer> pidList;
    Log.info("Enforcing DiskUsage");
    pidList = procReader.filteredActivePIDList(exceptions);
    for (int storedPID : new HashSet<Integer>(diskUsageMap.keySet())) {
      if (!pidList.contains(storedPID)) {
        diskUsageMap.remove(storedPID);
      }
    }
    for (int pid : pidList) {
      checkDiskUsage(pid);
    }
    return intervalMillis;
  }

  private void checkDiskUsage(int pid) {
    long used;

    used = getDiskUsage(pid);
    if (used > diskUsageLimit) {
      String msg = new String("Disk usage limit exceeded: " + pid + "\t" + used);
      Log.warning(msg);
      terminator.terminate(pid, msg);
    }
  }

  private long getDiskUsage(int pid) {
    List<String> fileNames;
    ProcessDiskUsage usage;

    usage = diskUsageMap.get(pid);
    if (usage == null) {
      usage = new ProcessDiskUsage();
      diskUsageMap.put(pid, usage);
    }
    fileNames = procReader.openFD(pid);
    for (String fileName : fileNames) {
      Path path;
      path = new File("/proc/" + pid + "/fd/" + fileName).toPath();
            /*
            System.out.println("\t"+ path 
                    +" "+ Files.isSymbolicLink(path) 
                    +" "+ Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
                    +" "+ Files.isRegularFile(path));
                    */
      if (Files.isSymbolicLink(path)) {
        try {
          String targetName;

          targetName = Files.readSymbolicLink(path).toString();
          if (isMeasuredFile(targetName)) {
            usage.addFile(targetName);
          }
        } catch (IOException ioe) {
          /***
           * Ignore
           */
        }
      } else {
        //System.out.println("Not symlink");
      }
    }
    return usage.currentUsage();
  }

  private boolean isMeasuredFile(String fileName) {
    for (String path : paths) {
      if (fileName.startsWith(path)) {
        return true;
      }
    }
    return false;
  }

  private static class ProcessDiskUsage {
    private final Set<String> existingFiles;

    ProcessDiskUsage() {
      existingFiles = new HashSet<>();
    }

    void addFile(String fileName) {
      existingFiles.add(fileName);
    }

    long currentUsage() {
      long usage = 0;
      Iterator<String> it = existingFiles.iterator();
      while (it.hasNext()) {
        File file;
        file = new File(it.next());
        if (file.exists()) {
          usage += file.length();
        } else {
          it.remove();
        }
      }
      return usage;
    }
  }

}
