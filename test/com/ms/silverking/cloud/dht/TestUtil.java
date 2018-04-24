package com.ms.silverking.cloud.dht;

import static com.ms.silverking.cloud.dht.NamespacePerspectiveOptions.standardVersionProvider;

import java.util.HashSet;

import com.ms.silverking.cloud.dht.ValueRetentionPolicy.ImplementationType;
import com.ms.silverking.cloud.dht.VersionConstraint.Mode;
import com.ms.silverking.cloud.dht.client.AbsMillisVersionProvider;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.client.ConstantVersionProvider;
import com.ms.silverking.cloud.dht.client.KeyDigestType;
import com.ms.silverking.cloud.dht.client.OpSizeBasedTimeoutController;
import com.ms.silverking.cloud.dht.client.WaitForTimeoutController;
import com.ms.silverking.cloud.dht.client.crypto.EncrypterDecrypter;
import com.ms.silverking.cloud.dht.client.crypto.XOREncrypterDecrypter;
import com.ms.silverking.cloud.dht.net.ForwardingMode;
import com.ms.silverking.time.ConstantAbsMillisTimeSource;

public class TestUtil {
	
	// *Copy: copies the values of nspOptions (copies the object values, rather than re-using the same object from defaultNspOptions, important for comparing reference vs value, i.e. hashCode and equals)
	// *Diff: copies the values of nspOptionsCopy and changes at least one value - this is so that it's different
	public static final Class<byte[]> kcCopy                   = byte[].class;
	public static final Class<IllegalArgumentException> kcDiff = IllegalArgumentException.class;
    
    public static final Class<byte[]> vcCopy            = byte[].class;
    public static final Class<Enum> vcDiff              = Enum.class;
    
    public static final KeyDigestType kdtCopy           = KeyDigestType.MD5;
    public static final KeyDigestType kdtDiff           = KeyDigestType.NONE;
    
    // using ImmutableSet.of() to add variety of param testing   
    public static final PutOptions poCopy               = new PutOptions(new OpSizeBasedTimeoutController(), null,            Compression.LZ4, ChecksumType.MURMUR3_32, false, 0, null);
    public static final PutOptions poDiff               = new PutOptions(new WaitForTimeoutController(),     new HashSet<>(), Compression.LZ4, ChecksumType.MURMUR3_32, false, 0, null);
    
    public static final InvalidationOptions ioCopy      = new InvalidationOptions(new OpSizeBasedTimeoutController(), null,            0);
    public static final InvalidationOptions ioDiff      = new InvalidationOptions(new WaitForTimeoutController(),     new HashSet<>(), 0);
    
    public static final GetOptions goCopy               = new GetOptions(new OpSizeBasedTimeoutController(), null,            RetrievalType.VALUE, new VersionConstraint(Long.MIN_VALUE, Long.MAX_VALUE, Mode.GREATEST), NonExistenceResponse.NULL_VALUE, true, false, ForwardingMode.FORWARD, false);
    public static final GetOptions goDiff               = new GetOptions(new WaitForTimeoutController(),     new HashSet<>(), RetrievalType.VALUE, new VersionConstraint(Long.MIN_VALUE, Long.MAX_VALUE, Mode.GREATEST), NonExistenceResponse.NULL_VALUE, true, false, ForwardingMode.FORWARD, false);
    
    public static final WaitOptions woCopy              = new WaitOptions(new WaitForTimeoutController(), null,            RetrievalType.VALUE,     new VersionConstraint(Long.MIN_VALUE, Long.MAX_VALUE, Mode.GREATEST), NonExistenceResponse.NULL_VALUE, true, false, false, Integer.MAX_VALUE, 100, TimeoutResponse.EXCEPTION);
    public static final WaitOptions woDiff              = new WaitOptions(new WaitForTimeoutController(), new HashSet<>(), RetrievalType.EXISTENCE, new VersionConstraint(Long.MIN_VALUE, Long.MAX_VALUE, Mode.GREATEST), NonExistenceResponse.NULL_VALUE, true, false, false, Integer.MAX_VALUE, 100, TimeoutResponse.EXCEPTION);
    
    public static final ConstantVersionProvider vpCopy  = new ConstantVersionProvider(standardVersionProvider.getVersion());
    public static final AbsMillisVersionProvider vpDiff = new AbsMillisVersionProvider(new ConstantAbsMillisTimeSource(0));
    
    public static final EncrypterDecrypter edCopy       = null;
    public static final EncrypterDecrypter edDiff       = new XOREncrypterDecrypter(new byte[]{});
    
    public static NamespacePerspectiveOptions<byte[], byte[]> getCopy() {
    	return new NamespacePerspectiveOptions<>(kcCopy, vcCopy, kdtCopy, poCopy, ioCopy, goCopy, woCopy, vpCopy, edCopy);
    }
    
    public static NamespacePerspectiveOptions<IllegalArgumentException, Enum> getDiff() {
    	return new NamespacePerspectiveOptions<>(kcDiff, vcDiff, kdtDiff, poDiff, ioDiff, goDiff, woDiff, vpDiff, edDiff);
    }
	
	public static ImplementationType getImplementationType(ValueRetentionPolicy policy) {
		return policy.getImplementationType();
	}
}
