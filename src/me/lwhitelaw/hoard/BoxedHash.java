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

	/**
	 * Construct a boxed hash with the provided array, taking ownership of the array object.
	 * @param value The hash array instance to use
	 */
	private BoxedHash(byte[] value) {
		if (value == null) throw new NullPointerException();
		hash = value;
	}
	
	/**
	 * Construct a boxed hash object from the provided hash. The array will be copied.
	 * @param hash The hash to use
	 * @return a boxed hash with the same value as the provided array.
	 */
	public static BoxedHash valueOf(byte[] hash) {
		byte[] h = new byte[hash.length];
		System.arraycopy(hash, 0, h, 0, h.length);
		return new BoxedHash(h);
	}
	
	/**
	 * Return the hash as a byte array. A new byte array will be created to hold the value.
	 * @return the hash value
	 */
	public byte[] hashValue() {
		byte[] h = new byte[hash.length];
		System.arraycopy(hash, 0, h, 0, h.length);
		return h;
	}
	
	/**
	 * Lexicographically compare two hashes. See {@link Hashes#compare(byte[], byte[])}.
	 */
	@Override
	public int compareTo(BoxedHash other) {
		return Hashes.compare(hash, other.hash);
	}
	
	/**
	 * Return true if two hashes have the same hash values.
	 */
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof BoxedHash)) return false;
		BoxedHash other = (BoxedHash) obj;
		return Arrays.equals(hash, other.hash);
	}
	
	/**
	 * Compute a hash code as the first 4 bytes of the hash value.
	 */
	@Override
	public int hashCode() {
		int smallhash = 0;
		for (int i = 0; i < Math.min(4, hash.length); i++) {
			smallhash = (smallhash << 8) | (hash[i] & 0xFF);
		}
		return smallhash;
	}
	
	/**
	 * Return the hash as a string. See {@link Hashes#hashToString(byte[])}.
	 */
	@Override
	public String toString() {
		return Hashes.hashToString(hash);
	}
}
