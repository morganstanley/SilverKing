package com.ms.silverking.cloud.argus;

/*
import static org.junit.Assert.*;

import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.ms.silverking.net.IPAddrUtil;
import com.ms.silverking.os.linux.proc.ProcReader;
import com.ms.silverking.os.linux.proc.ProcTreeNode;
import com.ms.silverking.thread.ThreadUtil;
*/
public class ProcessTest {
    /*
    private final List<String>  priorityMasters;
    private final List<String>  guestMasters;
    private final List<String>  guestMasterProxies;
    private final List<String>  priorityChildrenToIgnore;
    private final ProcReader    procReader;
    private final Terminator    terminator;
    
    private enum GuestState {UNKNOWN, INACTIVE, ACTIVE, CONFLICTED};
    
    private static final int    activeDepth = 1;
    private static final boolean    debug = false;
    private static final String thisClassPattern = ".*ProcessTest.*";
    private static final List<String>   masterExclusionList;
    
    private static final String  defaultArgs = "Armed 10 5 .*NodeDaemon.* .*symphony.*pem .*platformShellSvc.*:.*ShellCommandService.*:.*3rd.*symphony.* .*gridtasks.*";
    //private static final StringdefaultArgs = "Armed 10 5 .*NodeDaemon.* .*symphony.*pem .*platformShellSvc.*:.*ShellCommandService.* .*gridtasks.*";
    //                                   terminatorMode inactiveIntervalSeconds> activeIntervalSeconds" +
    //                                   priorityMaster:priorityMaster:...> <guestMaster:guestMaster:...> <guestMasterProxies> <priorityChildrenToIgnore>");
    
    static {
        masterExclusionList = new ArrayList<>();
        masterExclusionList.add(thisClassPattern);
    }
    
    public ProcessTest(Terminator.Mode terminatorMode, 
                       List<String> priorityMasters, 
                       List<String> guestMasters, 
                       List<String> guestMasterProxies,
                       List<String> priorityChildrenToIgnore) {
        this.priorityMasters = priorityMasters;
        this.guestMasters = guestMasters;
        this.guestMasterProxies = guestMasterProxies;
        this.priorityChildrenToIgnore = priorityChildrenToIgnore;
        procReader = new ProcReader();
        terminator = new Terminator(terminatorMode);
    }
    
    public void ensureIntegrity(int inactiveIntervalSeconds, int activeIntervalSeconds) {
        boolean     running;
        GuestState  guestState;
        
        guestState = GuestState.UNKNOWN;
        running = true;
        while (running) {
            int sleepSeconds;
            
            guestState = checkIntegrity(guestState);
            switch (guestState) {
            case ACTIVE:
            case CONFLICTED:
                sleepSeconds = activeIntervalSeconds;
                break;
            default: sleepSeconds = inactiveIntervalSeconds;
            }
            ThreadUtil.sleepSeconds(sleepSeconds);
        }
    }
    
    private List<Integer> getMasterPIDList(List<Integer> pidList, List<String> masters) {
        pidList = procReader.filteredActivePIDList(pidList, ProcReader.FilterType.INCLUSIVE, masters);        
        pidList = procReader.filteredActivePIDList(pidList, ProcReader.FilterType.EXCLUSIVE, masterExclusionList);
        return pidList;
    }
    
    private List<Integer> checkForProxies(Map<Integer,ProcTreeNode> procForest, List<Integer> pidList, List<String> proxyPatterns) {
        List<Integer>  proxyPIDList;
        
        proxyPIDList = new ArrayList<>();
        for (int pid : pidList) {
            ProcTreeNode    node;
            boolean         proxyFound;
            
            proxyFound = false;
            node = procForest.get(pid);
            if (node != null) {
                for (ProcTreeNode child : node.getChildren()) {
                    if (procReader.cmdLineMatchesPattern(child.getPID(), proxyPatterns)) {
                        proxyFound = true;
                        //System.out.println(pid +" has proxy "+ child.getPID());
                        proxyPIDList.add(child.getPID());
                    } else {
                        //System.out.println("no child proxy match: "+ child.getPID());
                    }
                }
            } else {
                // must be an inconsistent tree, just let the next sample fix it
            }
            if (!proxyFound) {
                //StringUtil.display(proxyPatterns);
                //System.out.println("no proxy match: "+ pid);
                proxyPIDList.add(pid);
            }
        }
        return proxyPIDList;
    }
    
    private GuestState checkIntegrity(GuestState oldGuestState) {
        Map<Integer,ProcTreeNode>   procForest;
        List<Integer>   activePIDs;
        List<Integer>   guestPIDs;
        GuestState      newGuestState;
        
        procForest = ProcTreeNode.buildForest(procReader.activeProcessStats());
        activePIDs = procReader.activePIDList();
        guestPIDs = getMasterPIDList(activePIDs, guestMasters);
        //guestPIDs = checkForProxies(procForest, getMasterPIDList(activePIDs, guestMasters), guestMasterProxies);
        //guestPIDs = procReader.filteredActivePIDList(activePIDs, ProcReader.FilterType.INCLUSIVE, guestMasters); 
        if (active(procForest, guestPIDs, guestMasterProxies)) {
            List<Integer>   priorityPIDs;
            
            priorityPIDs = getMasterPIDList(activePIDs, priorityMasters);
            //priorityPIDs = procReader.filteredActivePIDList(activePIDs, ProcReader.FilterType.INCLUSIVE, priorityMasters);
            //System.out.println(priorityPIDs.get(0) +"\t"+ procForest.get(priorityPIDs.get(0)).getDepth());
            if (active(procForest, priorityPIDs, null)) {
                newGuestState = GuestState.CONFLICTED;
                displayMasterChildren(procForest, guestPIDs);
                terminateMasterChildren(procForest, guestPIDs);
            } else {
                newGuestState = GuestState.ACTIVE;
            }
        } else {
            newGuestState = GuestState.INACTIVE;
            if (debug) {
                if (active(procForest, procReader.filteredActivePIDList(activePIDs, ProcReader.FilterType.INCLUSIVE, priorityMasters), null)) {
                    System.out.println("Priority is active");
                } else {
                    System.out.println("Priority is not active");
                }
            }
        }
        if (debug) {
            System.err.println(new Date() +"\t"+ oldGuestState +"\t"+ newGuestState); 
            System.err.flush();
        }
        if (newGuestState != oldGuestState) {
            System.out.println(IPAddrUtil.addrToString(IPAddrUtil.localIP()) +"\t"+ new Date() +"\t"+ oldGuestState +"\t"+ newGuestState); 
            System.out.flush();
        }
        return newGuestState;
    }
    
    private void terminateMasterChildren(Map<Integer,ProcTreeNode> procForest, List<Integer> masterPIDs) {
        for (int masterPID : masterPIDs) {
            ProcTreeNode    node;
            
            node = procForest.get(masterPID);
            if (node != null) {
                for (ProcTreeNode child : node.getChildren()) {
                    terminateTree(child);
                }
            }
        }
    }
       
    private void terminateTree(ProcTreeNode node) {
        terminator.terminate(node.getPID());
        for (ProcTreeNode child : node.getChildren()) {
            terminateTree(child);
        }
    }
    
    private void displayMasterChildren(Map<Integer,ProcTreeNode> procForest, List<Integer> masterPIDs) {
        for (int masterPID : masterPIDs) {
            ProcTreeNode    node;
            
            node = procForest.get(masterPID);
            if (node != null) {
                for (ProcTreeNode child : node.getChildren()) {
                    displayTree(child);
                }
            }
        }
    }
    
    private void displayTree(ProcTreeNode node) {
        System.out.println(node.getPID() +"\t"+ procReader.readCommandLine(node.getPID()));
        for (ProcTreeNode child : node.getChildren()) {
            displayTree(child);
        }
    }
    
    private boolean active(Map<Integer,ProcTreeNode> procForest, List<Integer> masterPIDs, List<String> ignoreList) {
        for (int masterPID : masterPIDs) {
            if (active(procForest, masterPID, ignoreList)) {
                return true;
            }
        }
        return false;
    }
    
    private boolean active(Map<Integer,ProcTreeNode> procForest, int masterPID, List<String> ignoreList) {
        ProcTreeNode    masterProc;
        
        masterProc = procForest.get(masterPID);
        //System.out.println(masterPID +"\t"+ (masterProc != null ? masterProc.getDepth() : "null"));
        return masterProc != null && getDepth(procForest, masterProc, ignoreList) >= activeDepth;
    }
    
    private int getDepth(Map<Integer,ProcTreeNode> procForest, ProcTreeNode node, List<String> ignoreList) {
        boolean protectedChildFound;
        int maxChildDepth;
        
        protectedChildFound = false;            
        maxChildDepth = 0;
        for (ProcTreeNode child : node.getChildren()) {
            List<String>    myIgnoreList;
            
            myIgnoreList = new ArrayList<>(priorityChildrenToIgnore);
            if (ignoreList != null) {
                myIgnoreList.addAll(ignoreList);
            }
            if (!procReader.cmdLineMatchesPattern(child.getPID(), myIgnoreList)) {
                maxChildDepth = Math.max(maxChildDepth, child.getDepth());
                protectedChildFound = true;
            }
        }
        return protectedChildFound ? maxChildDepth + 1 : 0;
    }
    
    public static void start(String[] args) {
        //System.out.println("v1"); System.out.flush();
        try {
            if (args.length != 7 && args.length > 1) {
                System.out.println("args: <terminatorMode> <inactiveIntervalSeconds> <activeIntervalSeconds> "
                        +"<priorityMaster:priorityMaster:...> <guestMaster:guestMaster:...> <guestMasterProxies> <priorityChildrenToIgnore>");
            } else {
                ProcessTest pt;
                String[]  priorityMasters;
                String[]  guestMasters;
                String[]  guestMasterProxies;
                String[]  priorityChildrenToIgnore;
                int       inactiveIntervalSeconds;
                int       activeIntervalSeconds;
                Terminator.Mode terminatorMode;
                
                if (args.length == 0) {
                    args = defaultArgs.split("\\s+");
                } else if (args.length == 1) {
                    args = FileUtil.readFileAsString(new File(args[0])).split("\\s+");
                }
                terminatorMode = Terminator.Mode.valueOf(args[0]);
                inactiveIntervalSeconds = Integer.parseInt(args[1]);
                activeIntervalSeconds = Integer.parseInt(args[2]);
                priorityMasters = args[3].split(":");
                guestMasters = args[4].split(":");
                guestMasterProxies = args[5].split(":");
                priorityChildrenToIgnore = args[6].split(":");
                pt = new ProcessTest(terminatorMode, 
                                    ImmutableList.copyOf(priorityMasters), 
                                     ImmutableList.copyOf(guestMasters),
                                     ImmutableList.copyOf(guestMasterProxies),
                                     ImmutableList.copyOf(priorityChildrenToIgnore));
                pt.ensureIntegrity(inactiveIntervalSeconds, activeIntervalSeconds);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void main(String[] args) {
        start(args);
    }
    */
}
