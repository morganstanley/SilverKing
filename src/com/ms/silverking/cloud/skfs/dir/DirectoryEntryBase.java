package com.ms.silverking.cloud.skfs.dir;

import com.ms.silverking.numeric.NumConversion;

public abstract class DirectoryEntryBase implements DirectoryEntry {
	protected static final int	DE_MAGIC = 0xadab;
	
	protected static final int	magicLength = NumConversion.BYTES_PER_SHORT;
	protected static final int	sizeLength = NumConversion.BYTES_PER_SHORT;
	protected static final int	statusLength = NumConversion.BYTES_PER_SHORT + 2; /* 2 is padding to match C alignment */
	protected static final int	versionLength = NumConversion.BYTES_PER_LONG;
	
	protected static final int	magicOffset = 0;
	protected static final int	sizeOffset = magicOffset + magicLength; // size is name length + terminator + alignment padding if necessary
	protected static final int	statusOffset = sizeOffset + sizeLength;
	protected static final int	versionOffset = statusOffset + statusLength;
	protected static final int	dataOffset = versionOffset + versionLength;
	
	protected static final int headerSize = dataOffset;
	
}
