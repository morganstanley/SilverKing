package com.ms.silverking.util.memory;

public interface JVMMemoryObserver {
	public void jvmMemoryLow(boolean isLow);
	public void jvmMemoryStatus(long bytesFree);
}
