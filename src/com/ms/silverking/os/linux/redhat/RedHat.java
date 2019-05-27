package com.ms.silverking.os.linux.redhat;

import java.io.File;

import com.ms.silverking.io.FileUtil;

public class RedHat {
	private static final File	versionFile = new File("/etc/redhat-release");
	private static final String	releaseString = "release ";
	
	public static final double	notRedHatLinux = 0.0;
	
	public static double getRedHatVersion() {
		try {
			String	version;
			int		i0;
			int		i1;
			
			version = FileUtil.readFileAsString(versionFile);
			i0 = version.indexOf(releaseString);
			if (i0 < 0) {
				return notRedHatLinux;
			}
			i1 = version.indexOf(' ', i0 + releaseString.length());
			if (i1 < 0) {
				return notRedHatLinux;
			}
			return Double.parseDouble(version.substring(i0 + releaseString.length(), i1));
		} catch (Exception e) {
			return notRedHatLinux;
		}
	}
	
	public static final void main(String[] args) {
		System.out.printf("RedHat Version %f\n", getRedHatVersion());
	}
}
