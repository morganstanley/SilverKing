package com.ms.silverking.cloud.skfs.dir.serverside;

import java.io.File;
import java.util.concurrent.BlockingQueue;

import com.ms.silverking.collection.LightLinkedBlockingQueue;
import com.ms.silverking.log.Log;

class FileDeletionWorker implements Runnable {
	private final BlockingQueue<File>	workQueue;
	
	private static final String	threadName = "FileDeletionWorker";
	
	public FileDeletionWorker() {
		Thread	t;
		
		workQueue = new LightLinkedBlockingQueue<>();
		t = new Thread(this, threadName);
		t.setDaemon(true);
		t.start();
	}

	@Override
	public void run() {
		while (true) {
			try {
				File	f;
				
				f = workQueue.take();
				if (f != null) {
					if (f.exists() && !f.delete()) {
						Log.warningf("%s unable to delete %s", threadName, f.getAbsolutePath());
					}
				}
			} catch (Exception e) {
				Log.logErrorWarning(e);
			}
		}
	}

	public void delete(File f) {
		try {
			workQueue.put(f);
		} catch (InterruptedException e) {
		}
	}
}
