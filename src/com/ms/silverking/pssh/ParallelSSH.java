package com.ms.silverking.pssh;

import java.util.Arrays;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.config.HostGroupTable;
import com.ms.silverking.io.StreamParser;
import com.ms.silverking.log.Log;

public class ParallelSSH extends ParallelSSHBase implements Runnable {
    private boolean running;
    private Queue<String> hosts;
    private String[] command;
    private int timeoutSeconds;
    private AtomicInteger   active;
    
    public ParallelSSH(Set<String> hosts, String[] command, 
                        int numWorkerThreads, int timeoutSeconds,
                        HostGroupTable hostGroups) {
    	super(hostGroups);
        this.hosts = new ArrayBlockingQueue<String>(hosts.size(), false, hosts);
        this.command = command;
        this.timeoutSeconds = timeoutSeconds;
        active = new AtomicInteger();
        running = true;
        for (int i = 0; i < numWorkerThreads; i++) {
            new Thread(this, "ParallelSSH Worker "+ i).start();
        }
    }
    
    public void run() {
        while (running) {
            String  host;
            
            host = hosts.poll();
            if (host == null) {
                break;
            } else {
                active.incrementAndGet();
                try {
                    Log.warning("Remaining: "+ hosts.size() +"\tActive: "+ active);
                    doSSH(host, command, timeoutSeconds);
                } finally {
                    Log.warning("Complete: "+ host);
                    active.decrementAndGet();
                }
            }
        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            Set<String>     hosts;
            int             numWorkerThreads;
            int             timeoutSeconds;
            String[]        cmd;
            ParallelSSH     parallelSSH;
            
            if (args.length < 4) {
                System.out.println("<hostsFile> <numWorkerThreads> <timeoutSeconds> <cmd...>");
                return;
            }
            hosts = ImmutableSet.copyOf(StreamParser.parseFileLines(args[0]));
            numWorkerThreads = Integer.parseInt(args[1]);
            timeoutSeconds = Integer.parseInt(args[2]);
            cmd = Arrays.copyOfRange(args, 3, args.length); 
            parallelSSH = new ParallelSSH(hosts, cmd, numWorkerThreads, timeoutSeconds, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
