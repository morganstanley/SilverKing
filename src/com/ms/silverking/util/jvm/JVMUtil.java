package com.ms.silverking.util.jvm;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.security.Permission;

import com.ms.silverking.thread.ThreadUtil;

public class JVMUtil {
    public static void dumpStackTraces() {
        ThreadInfo[] threads;
        
        threads = ManagementFactory.getThreadMXBean().dumpAllThreads(true, true);
        
        ThreadUtil.printStackTraces();
        System.out.println();
        for (ThreadInfo thread : threads) {
            long    threadID;
            
            threadID = thread.getThreadId();
            System.out.println("ThreadId:" + thread.getThreadId() + " "+ "ThreadName:" + thread.getThreadName()+ " "+"ThreadState:"
            + thread.getThreadState()+ " "+"BlockedCount:" + thread.getBlockedCount()+ " "+"WaitedCount: " + thread.getWaitedCount());
    
            if (thread.getLockOwnerId()>-1) {
                System.out.println("LOCK INFORMATION For:" + threadID);
                System.out.println("LockName:"+ thread.getLockOwnerName()+ " " +"LockOwnerName:"+ thread.getLockName());
            }
        }
    }
    

    public static void debugSystemExitCall() {
		final SecurityManager securityManager = new SecurityManager() {
			public void checkPermission(Permission permission) {
				if (permission.getName().startsWith("exitVM")) {
					Thread.dumpStack();
				}
			}
		};
		System.setSecurityManager(securityManager);
    }    
        
    /**
     * @param args
     */
    public static void main(String[] args) {
        dumpStackTraces();
    }
}
