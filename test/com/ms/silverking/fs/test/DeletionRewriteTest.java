package com.ms.silverking.fs.test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.ms.silverking.collection.Pair;
import com.ms.silverking.time.SimpleTimer;
import com.ms.silverking.time.Timer;

/**
 * Simple test to stress file creation and deletion
 */
public class DeletionRewriteTest {
	private final Test	test;
	private final int	numFiles;
	private final int	durationMillis;
	private final int	numThreads;
	private final Lock[]	locks;
	
	public enum Test {Create, RandomDeletion};
	
	public DeletionRewriteTest(Test test, int numFiles, int durationSeconds, int numThreads) {
		this.test = test;
		this.numFiles = numFiles;
		this.durationMillis = durationSeconds * 1000;
		this.numThreads = numThreads;
		locks = new Lock[numFiles];
		for (int i = 0; i < locks.length; i++) {
			locks[i] = new ReentrantLock();
		}
	}
	
	public void runTest() throws IOException {
		switch (test) {
		case Create:
			create();
			break;
		case RandomDeletion:
			randomDeletion();
			break;
		default:
			throw new RuntimeException("panic");
		}
	}
	
	private File getFile(int index) {
		return new File(Integer.toString(index));
	}
	
	private void create() throws IOException {
		for (int i = 0; i < numFiles; i++) {
			getFile(i).createNewFile();
		}
	}
	
	private void randomDeletion() {
		TestRunner[]	testRunners;
		int				totalGood;
		int				totalBad;
		
		totalGood = 0;
		totalBad = 0;
		testRunners = new TestRunner[numThreads];		
		for (int i = 0; i < testRunners.length; i++) {
			testRunners[i] = new TestRunner();
		}
		for (int i = 0; i < testRunners.length; i++) {
			Pair<Integer,Integer>	results;
			
			results = testRunners[i].waitForCompletion();
			totalGood += results.getV1();
			totalBad += results.getV2();
		}
		System.out.printf("totalGood:\t%d\n", totalGood);
		System.out.printf("totalBad: \t%d\n", totalBad);
	}
	
	class TestRunner implements Runnable {
		private final Thread	thread;
		private int	good;
		private int	bad;
		private volatile boolean	complete;
		
		TestRunner() {
			thread = new Thread(this);
			thread.start();
		}
		
		public void run() {
			try {
				Timer	timer;
				
				timer = new SimpleTimer(TimeUnit.MILLISECONDS, durationMillis);
				while (!timer.hasExpired()) {
					try {
						switch (test) {
						case Create:
							throw new RuntimeException("Create should not call TestRunner");
						case RandomDeletion:
							randomlyRewrite();
							break;
						default:
							throw new RuntimeException("panic");
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			} finally {
				complete = true;
				synchronized (this) {
					this.notifyAll();
				}
			}
		}

		private void randomlyRewrite() throws IOException {
			File	f;
			boolean	created;
			int		index;
			
			index = ThreadLocalRandom.current().nextInt(numFiles);
			f = getFile(index);
			locks[index].lock();
			try {
				f.delete();
				created = f.createNewFile();
			} finally {
				locks[index].unlock();
			}
			if (!created) {
				System.out.printf("Failed to rewrite %s\n", f.getName());
				++bad;
			} else {
				++good;
			}
		}
		
		public Pair<Integer,Integer> waitForCompletion() {
			synchronized (this) {
				while (!complete) {
					try {
						this.wait();
					} catch (InterruptedException e) {
					}
				}
			}
			return new Pair<>(good, bad);
		}
	}

	public static void main(String[] args) {
		if (args.length != 2 && args.length != 4) {
			System.out.println("args: <test> <numFiles> [durationSeconds] [threads]");
		} else {
			try {
				Test	test;
				int		numFiles;
				int		durationSeconds;
				int		numThreads;
				DeletionRewriteTest	deletionRewriteTest;
				
				test = Test.valueOf(args[0]);
				numFiles = Integer.parseInt(args[1]);
				if (test == Test.RandomDeletion) {
					durationSeconds = Integer.parseInt(args[2]);
					numThreads = Integer.parseInt(args[3]);
				} else {
					durationSeconds = 0;
					numThreads = 0;
				}
				deletionRewriteTest = new DeletionRewriteTest(test, numFiles, durationSeconds, numThreads);
				deletionRewriteTest.runTest();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
