package com.ms.silverking.net;

public class AddrAndPortUtil {
    public static String toString(AddrAndPort[] aps) {
        StringBuilder   sb;
        boolean         first;
        
        sb = new StringBuilder();
        first = true;
        for (AddrAndPort ap : aps) {
            if (!first) {
                sb.append(AddrAndPort.multipleDefDelimiter);
            } else {
                first = false;
            }
            sb.append(ap.toString());
        }
        return sb.toString();
    }

	public static int hashCode(AddrAndPort[] ensemble) {
		int	hashCode;
		
		hashCode = 0;
    	for (AddrAndPort member : ensemble) {
    		hashCode = hashCode ^ member.hashCode();
    	}
    	return hashCode;
	}
}
