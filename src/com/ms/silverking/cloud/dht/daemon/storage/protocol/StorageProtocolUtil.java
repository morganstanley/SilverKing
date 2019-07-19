package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import com.ms.silverking.cloud.dht.ConsistencyProtocol;

public class StorageProtocolUtil {
    // FUTURE - Make this protocol-specific
    public static final byte   initialStorageStateOrdinal = 0;
    
    public static boolean storageStateValidForRead(ConsistencyProtocol consistencyProtocol, byte storageState) {
        switch (consistencyProtocol) {
        case LOOSE: return true;
        case TWO_PHASE_COMMIT: return TwoPhaseStorageState.values()[storageState].validForRead();
        default: throw new RuntimeException("panic");
        }
    }
    
    public static boolean requiresStorageStateVerification(ConsistencyProtocol cp) {
        switch (cp) {
        case LOOSE: return false;
        case TWO_PHASE_COMMIT: return true;
        default: throw new RuntimeException("panic");
        }
    }
}
