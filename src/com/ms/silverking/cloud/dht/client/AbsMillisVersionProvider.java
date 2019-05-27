package com.ms.silverking.cloud.dht.client;

import com.ms.silverking.time.AbsMillisTimeSource;

/**
 * Provides versions from an AbsMillisTimeSource.
 */
public class AbsMillisVersionProvider implements VersionProvider {
	private final AbsMillisTimeSource absMillisTimeSource;

	public AbsMillisVersionProvider(AbsMillisTimeSource absMillisTimeSource) {
		this.absMillisTimeSource = absMillisTimeSource;
	}

	@Override
	public long getVersion() {
		return absMillisTimeSource.absTimeMillis();
	}

	@Override
	public int hashCode() {
		return absMillisTimeSource.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (this.getClass() != o.getClass()) {
			return false;
		}

		AbsMillisVersionProvider other = (AbsMillisVersionProvider) o;
		return absMillisTimeSource.equals(other.absMillisTimeSource);
	}
}
