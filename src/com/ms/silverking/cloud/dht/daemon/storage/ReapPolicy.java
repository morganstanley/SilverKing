package com.ms.silverking.cloud.dht.daemon.storage;

public interface ReapPolicy<T extends ReapPolicyState> {
	public enum EmptyTrashMode {Never, BeforeInitialReap, BeforeAndAfterInitialReap, EveryPartialReap, EveryFullReap};
	
	public T createInitialState();	
	public boolean reapAllowed(T state, NamespaceStore nsStore, boolean isStartup);
	public EmptyTrashMode getEmptyTrashMode();
	public int getIdleReapPauseMillis();
	public boolean supportsLiveReap();
	public int getReapIntervalMillis();
}
