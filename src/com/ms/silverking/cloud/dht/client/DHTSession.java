package com.ms.silverking.cloud.dht.client;

import com.ms.silverking.cloud.dht.GetOptions;
import com.ms.silverking.cloud.dht.NamespaceCreationOptions;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespacePerspectiveOptions;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.WaitOptions;

/**
 * Represents a client's session with the DHT. 
 * Namespaces and associated perspectives may be obtained from this session.
 * @param <K> key type
 * @param <V> value type
 * 
 * @see BaseNamespacePerspective
 */
public interface DHTSession {
    /**
     * Return the NamespaceCreationOptions specified for this DHT
     * @return the NamespaceCreationOptions specified for this DHT
     */
    public NamespaceCreationOptions getNamespaceCreationOptions();
    /**
     * Return the default NamespaceOptions for this SK instance
     * @return the default NamespaceOptions for this SK instance
     */
    public NamespaceOptions getDefaultNamespaceOptions();
    /**
     * Shortcut for getDefaultNamespaceOptions().getDefaultPutOptions()
     * @return default PutOptions
     */
    public PutOptions getDefaultPutOptions();
    /**
     * Shortcut for getDefaultNamespaceOptions().getDefaultGetOptions()
     * @return default GetOptions
     */
    public GetOptions getDefaultGetOptions();
    /**
     * Shortcut for getDefaultNamespaceOptions().getDefaultWaitOptions()
     * @return default WaitOptions
     */
    public WaitOptions getDefaultWaitOptions();
    /**
     * Create a namespace with default NamespaceOptions
     * @param namespace name of the namespace
     * @return
     * @throws NamespaceCreationException if the namespace already exists
     */
    public Namespace createNamespace(String namespace) throws NamespaceCreationException;
    /**
     * Create a namespace with the specified options
     * @param namespace name of the namespace
     * @param nsOptions NamespaceOptions used for creating this namespace. 
     * If null, default this instance's NamespaceOptions will be used
     * @return
     * @throws NamespaceCreationException if the namespace already exists
     */
    public Namespace createNamespace(String namespace, NamespaceOptions nsOptions) throws NamespaceCreationException;
    /**
     * Get a previously created Namespace
     * @param namespace
     * @return
     */
    public Namespace getNamespace(String namespace);
    /**
     * Open an AsynchronousNamespacePerspective for the given key, value types
     * @param <K> key type of perspective to open
     * @param <V> value type of perspective to open
     * @param namespace name of the namespace
     * @param nspOptions options for NamespacePerspective
     * @return
     */
	public <K,V> AsynchronousNamespacePerspective<K,V> openAsyncNamespacePerspective(String namespace, 
	                                                            NamespacePerspectiveOptions<K,V> nspOptions);	
    /**
     * Open an AsynchronousNamespacePerspective for the given key, value types
     * @param <K> key type of perspective to open
     * @param <V> value type of perspective to open
     * @param namespace name of the namespace
     * @param keyClass class of keys
     * @param valueClass class of values
     * @return
     */
	public <K,V> AsynchronousNamespacePerspective<K,V> openAsyncNamespacePerspective(String namespace, 
	                                                            Class<K> keyClass, Class<V> valueClass);	
	public <K,V> AsynchronousNamespacePerspective<K,V> openAsyncNamespacePerspective(String namespace);	
	/**
	 * Open a SynchronousNamespacePerspective for the given key, value types
	 * @param <K> key type of perspective to open
	 * @param <V> value type of perspective to open
	 * @param namespace name of the namespace
     * @param nspOptions options for NamespacePerspective
	 * @return
	 */
	public <K,V> SynchronousNamespacePerspective<K,V> openSyncNamespacePerspective(String namespace,
                                                                NamespacePerspectiveOptions<K,V> nspOptions);
    /**
     * Open a SynchronousNamespacePerspective for the given key, value types
     * @param <K> key type of perspective to open
     * @param <V> value type of perspective to open
     * @param namespace name of the namespace
     * @param keyClass class of keys
     * @param valueClass class of values
     * @return
     */
	public <K,V> SynchronousNamespacePerspective<K,V> openSyncNamespacePerspective(String namespace,
																Class<K> keyClass, Class<V> valueClass);
	public <K,V> SynchronousNamespacePerspective<K,V> openSyncNamespacePerspective(String namespace);
	/**
	 * Deletes an entire namespace. This causes the data in the namespace to be inaccessible, and the data  
	 * to be moved to the trash directory.
	 * @param namespace
	 */
	public void deleteNamespace(String namespace) throws NamespaceDeletionException;
	/**
	 * Attempts recovery of a deleted namespace. This is only possible of all namespace data is available in
	 * the trash directory.
	 * @param namespace
	 */
    public void recoverNamespace(String namespace) throws NamespaceRecoverException;
	
	/**
	 * Close the session
	 */
	public void close();
}
