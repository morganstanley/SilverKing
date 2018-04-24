package com.ms.silverking.cloud.skfs.dir;

import com.ms.silverking.numeric.NumConversion;

public class FileStatus {
	public static final int SIZE_BYTES = NumConversion.BYTES_PER_SHORT;
	
	public static final int	F_DELETED = 1;
	public static final String	S_DELETED = "deleted";
	public static final String	S_NOT_DELETED = "";


	public static int getDeleted(short fs) {
		return fs & F_DELETED;
	}

	public static short setDeleted(short fs, int deleted) {
		if (deleted != 0) {
			return (short)(fs | F_DELETED);
		} else {
			return (short)(fs & (~F_DELETED));
		}
	}

	public static String toString(short fs) {
	    if (getDeleted(fs) != 0) {
	        return S_DELETED;
	    } else {
	        return S_NOT_DELETED;
	    }
	}
}
