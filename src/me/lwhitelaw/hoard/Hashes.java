package me.lwhitelaw.hoard;

/**
 * Utilities for working with hashes.
 *
 */
public final class Hashes {
	private Hashes() {}

	private static final String HEX_DIGITS = "0123456789ABCDEF";
	
	/**
	 * Convert a hex character to a number.
	 * @param h character to convert
	 * @return value of this character
	 * @throws IllegalArgumentException if the character is not a valid hex digit
	 */
	private static int hexCharToInt(char h) {
		switch (h) {
			case '0': return 0x0;
			case '1': return 0x1;
			case '2': return 0x2;
			case '3': return 0x3;
			case '4': return 0x4;
			case '5': return 0x5;
			case '6': return 0x6;
			case '7': return 0x7;
			case '8': return 0x8;
			case '9': return 0x9;
			case 'a': case 'A': return 0xA;
			case 'b': case 'B': return 0xB;
			case 'c': case 'C': return 0xC;
			case 'd': case 'D': return 0xD;
			case 'e': case 'E': return 0xE;
			case 'f': case 'F': return 0xF;
		}
		throw new IllegalArgumentException("not a digit");
	}
	
	/**
	 * Convert a byte-array hash to a string representation.
	 * @param hash hash to convert
	 * @return the hash as a string
	 */
	public static String hashToString(byte[] hash) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < hash.length; i++) {
			int b = hash[i];
			sb.append(HEX_DIGITS.charAt((b >>> 4) & 0x0F));
			sb.append(HEX_DIGITS.charAt((b >>> 0) & 0x0F));
		}
		return sb.toString();
	}
	
	/**
	 * Convert a string hash to a byte-array representation
	 * @param hash string to convert
	 * @return the hash as a byte array
	 * @throws IllegalArgumentException if the string is not of appropriate length
	 */
	public static byte[] stringToHash(String hash) {
		if (hash.length() % 2 != 0) throw new IllegalArgumentException("hash string not a multiple of 2");
		byte[] out = new byte[hash.length() / 2];
		int o = 0;
		for (int i = 0; i < hash.length(); i += 2) {
			out[o] = (byte) (hexCharToInt(hash.charAt(i)) << 4 | hexCharToInt(hash.charAt(i+1)));
			o++;
		}
		return out;
	}
}
