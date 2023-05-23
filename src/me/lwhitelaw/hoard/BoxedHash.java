package me.lwhitelaw.hoard;

import java.util.Arrays;

/**
 * A "boxed" byte array type for hashes, suitable for use in collections that
 * demand comparators, equality, or hash codes. In most cases, the Hashes class
 * may be used instead.
 *
 */
public final class BoxedHash implements Comparable<BoxedHash> {
	private final byte[] hash;

	private BoxedHash(byte[] value) {
		if (value == null) throw new NullPointerException();
		hash = value;
	}
	
	public static BoxedHash valueOf(byte[] hash) {
		byte[] h = new byte[hash.length];
		System.arraycopy(hash, 0, h, 0, h.length);
		return new BoxedHash(h);
	}
	
	public byte[] hashValue() {
		byte[] h = new byte[hash.length];
		System.arraycopy(hash, 0, h, 0, h.length);
		return h;
	}
	
	@Override
	public int compareTo(BoxedHash other) {
		return Hashes.compare(hash, other.hash);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof BoxedHash)) return false;
		BoxedHash other = (BoxedHash) obj;
		return Arrays.equals(hash, other.hash);
	}
	
	@Override
	public int hashCode() {
		int smallhash = 0;
		for (int i = 0; i < Math.min(4, hash.length); i++) {
			smallhash = (smallhash << 8) | (hash[i] & 0xFF);
		}
		return smallhash;
	}
	
	@Override
	public String toString() {
		return Hashes.hashToString(hash);
	}
}
