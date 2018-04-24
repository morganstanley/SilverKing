package com.ms.silverking.pssh;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import com.ms.silverking.log.Log;
import com.ms.silverking.thread.ThreadUtil;

public class TwoLevelParallelSSHWorker implements Runnable {
    private ParallelSSHBase sshBase;
    
    private static final int    resultErrorCode = 127;

    private boolean running;
    private int timeoutSeconds;
    private AtomicInteger active;
    private AtomicInteger runningThreads;
    private String  myHost;

    private SSHMaster sshMaster;
    
    static {
        String  base;
        String  myStderr;
        String  myStdout;
        
        base = "/tmp/worker."+ System.currentTimeMillis();        
        myStdout = base +".stdout";
        myStderr = base +".stderr";
        try {
            System.setOut(new PrintStream(new FileOutputStream(myStdout)));
            System.setErr(new PrintStream(new FileOutputStream(myStderr)));
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
        }        
    }

    public TwoLevelParallelSSHWorker(String masterURL, 
            int numWorkerThreads, int timeoutSeconds) throws Exception {
        Runtime.getRuntime().addShutdownHook(new ShutdownHook());
        
        this.timeoutSeconds = timeoutSeconds;

        active = new AtomicInteger();
        runningThreads = new AtomicInteger();
        
        myHost = InetAddress.getLocalHost().getCanonicalHostName();

        sshMaster = findMaster(masterURL);
        sshBase = new ParallelSSHBase(null, sshMaster.getSSHCmdMap(), sshMaster.getHostGroups());

        if (sshMaster == null) {
            throw new RuntimeException("Master not found: " + masterURL);
        }

        runningThreads.set(numWorkerThreads);
        running = true;
        for (int i = 0; i < numWorkerThreads; i++) {
            new Thread(this, "ParallelSSH Worker " + i).start();
        }
    }

    private SSHMaster findMaster(String masterURL) throws Exception {
        return (SSHMaster) Naming.lookup(masterURL);
    }

    private HostAndCommand getHostAndCommand() throws RemoteException {
        return sshMaster.getHostAndCommand();
    }

    public void run() {
        try {
            while (running) {
                try {
                    HostAndCommand	hostAndCommand;
                    HostResult  	result;

                    Log.warning("Calling getHost");
                    hostAndCommand = getHostAndCommand();
                    Log.warning("back from getHost");
                    if (hostAndCommand == null) {
                        break;
                    } else {
                        active.incrementAndGet();
                        try {
                            int resultCode;
                            
                            Log.warning("\tHost: "+ hostAndCommand +"\tActive: " + active);
                            resultCode = sshBase.doSSH(hostAndCommand, timeoutSeconds, true);
                            result = resultCode == resultErrorCode ? result = HostResult.failure : HostResult.success;
                        } finally {
                            active.decrementAndGet();
                        }
                        sshMaster.setHostResult(hostAndCommand, result);
                    }
                } catch (RemoteException re) {
                    Log.logErrorWarning(re);
                    break;
                }
            }
        } finally {
            int running;
            
            System.out.println("WorkerThread complete "+ myHost);  System.out.flush();
            Log.warning("WorkerThread complete");
            running = runningThreads.decrementAndGet();
            if (running == 0) {
                try {
                    sshMaster.workerComplete();
                    System.out.println("Worker complete "+ myHost +" "+ new Date()); System.out.flush();
                    Log.warning("Worker complete");
                } catch (RemoteException re) {
                    Log.logErrorWarning(re);
                    ThreadUtil.pauseAfterException();
                }
            }
        }
    }
    
    public class ShutdownHook extends Thread {
        public void ShutdownHook() {
        }
        
        public void run() {
            Log.warning("Shutdown");
        }
    }

    // ////////////////////////////////////////////////////////////////////

    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            String masterURL;
            int numWorkerThreads;
            int timeoutSeconds;
            TwoLevelParallelSSHWorker parallelSSH;
            
            if (args.length != 3) {
                System.out.println("<masterURL> <numWorkerThreads> <timeoutSeconds>");
                return;
            }
            masterURL = args[0];
            numWorkerThreads = Integer.parseInt(args[1]);
            timeoutSeconds = Integer.parseInt(args[2]);
            parallelSSH = new TwoLevelParallelSSHWorker(masterURL, numWorkerThreads, timeoutSeconds);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
