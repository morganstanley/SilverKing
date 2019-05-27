package com.ms.silverking.fs;

import static com.ms.silverking.fs.TestUtil.setupAndCheckTestsDirectory;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoField;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ms.silverking.process.ProcessExecutor;
import com.ms.silverking.testing.Util;
import com.ms.silverking.testing.annotations.SkfsLarge;

@SkfsLarge
public class MemLeakTest {

    private static final int sevenMinsInMillis = 420_000;
    
    private static String testsDirPath;

    static {
        testsDirPath = TestUtil.getTestsDir();
    }
    
    private static final String memLeakDirName = "mem-leak";
    private static final File   memLeakDir     = new File(testsDirPath, memLeakDirName);
    
    @BeforeClass
    public static void setUpBeforeClass() {
        setupAndCheckTestsDirectory(memLeakDir);
    }
    
    // FUTURE - could put a timeout on create100kFiles too. 100k is taking around ~6 mins to generate. so if we have a runtime that's lower than 6 mins. doesn't make sense to keep generating files after this script is done..
    // dumping constantlyFindTheFiles to a file so that we can cross check that it is still running during our iteration loop times below. if the iteration loop sizes plateau it's b/c something is wrong and we aren't reading the files.
    @Test(timeout=sevenMinsInMillis)
    public void testMemLeak() throws InterruptedException {
        int bufferMins  = 2;
        int runtimeMins = toMins(sevenMinsInMillis) - bufferMins;
        
        // ProcessExcecutor doesn't like semi's after 'do;' for whatever reason, even though in terminal it works for both 'for' and 'while'
        String create100kFiles        = "for i in {1..100000}; do touch " + memLeakDir.getAbsolutePath() + "/$i; done";        // not doing 'cd memLeakDir.getAbsolutePath(); for i in ... touch $i ...; b/c if cd doesn't happen for some reason, I don't want to create 100k files wherever this script runs, so using full path when doing touch    
        // using a count instead of while [ 1 ], just in case the timeout doesn't work, this way the script will still end at some point
//        String constantlyFindTheFiles = "timeout " + toSeconds(runtimeMins) + " bash -c 'count=0; while [ $count -lt 200 ]; do find " + memLeakDir.getAbsolutePath() + "; ((count++)); echo \"`date +%H:%M:%S` - $count\" >> /tmp/memLeakTest_"+time+".out; sleep 1; done'";    // putting the >> inside the loop doesn't work right. it writes to the file 3 times, and then while the iteration for loop below is running it doesn't write anything, but if you kill the iteration for loop after it's in progress, then once it's killed, this while loop starts writing to the file again... and it's writing every second, which it shouldn't be b/c it takes time to find on all the files
        String constantlyFindTheFiles = "timeout " + toSeconds(runtimeMins) + " bash -c 'count=0; while [ $count -lt 200 ]; do find " + memLeakDir.getAbsolutePath() + "; ((count++)); echo \"`date +%H:%M:%S` - $count\"; sleep 1; done > /tmp/memLeakTest_"+getHourMinSec()+".out'";    // includes all the output, including from the 'find' command
        ProcessExecutor.runBashCmdNoWait(create100kFiles);
        ProcessExecutor.runBashCmdNoWait(constantlyFindTheFiles);

        int sleepSeconds      = 15;
        int numIterations     = toSeconds(runtimeMins) / sleepSeconds;
        int iterationsPerMin  = 60 / sleepSeconds;
        int[] iterationOutput = new int[numIterations];
        for (int i = 0; i < numIterations; i++) {
            String output = ProcessExecutor.runBashCmd("ps -ewwo rss,command | grep skfsd | grep -v grep | cut -f 1 -d ' '").trim();
            int intOutput = Integer.parseInt(output);
            iterationOutput[i] = intOutput;
            
            int minuteId          = i / iterationsPerMin;
            int minuteIterationId = (i % iterationsPerMin) * sleepSeconds;
            System.out.printf("%s - %2s.%-2s: %8d\n", ProcessExecutor.runCmd("date +%H:%M:%S").trim(), minuteId, minuteIterationId, intOutput);
            Thread.sleep(toMillis(sleepSeconds));
        }
        
        int diff = iterationOutput[iterationOutput.length-1] - iterationOutput[0];
        double gbPerHour = toGBPerHour(toGB(diff) / runtimeMins);
        System.out.printf("\nleaking @ %.2f GB/hr\n", gbPerHour);
        double thresholdGBPerHour = 20;
        Assert.assertTrue(gbPerHour < thresholdGBPerHour);
    }
    
    private int toMins(int minsInMillis) {
        int oneMinInMillis = 60 * 1000;
        return minsInMillis / oneMinInMillis;
    }
    
    private int toSeconds(int mins) {
        return mins * 60;
    }
    
    private int toMillis(int secs) {
        return secs * 1000;
    }
    
    private double toGB(int kb) {
        return (double)kb / 1_000_000;
    }
    
    private double toGBPerHour(double gbPerMin) {
        return gbPerMin * 60;
    }
    
    private String getHourMinSec() {
        LocalDateTime now = LocalDateTime.now();
        int year   = now.getYear();
        int month  = now.getMonthValue();
        int day    = now.getDayOfMonth();
        int hour   = now.getHour();
        int minute = now.getMinute();
        int second = now.getSecond();
        int millis = now.get(ChronoField.MILLI_OF_SECOND); // Note: no direct getter available.

        System.out.printf("%d-%02d-%02d %02d:%02d:%02d.%03d", year, month, day, hour, minute, second, millis);
        return System.out.format("%02d-%02d-%02d.%03d", year, month, day, hour, minute, second, millis).toString();
    }
    
    public static void main(String[] args) throws IOException {
        if (args.length == 1)
            testsDirPath = TestUtil.getTestsDir( args[0] );
        
        Util.println("Running tests in: " + testsDirPath);
        Util.runTests(MemLeakTest.class);
    }
}
