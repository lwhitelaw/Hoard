package me.lwhitelaw.hoard;

import java.nio.ByteBuffer;

/**
 * Compression utilities.
 *
 */
class Compression {
	private Compression() {}
	
	/**
	 * Determine heuristically if a given input is compressible, by attempting to predict
	 * successive bytes based on the previous byte read (order-1 context model).
	 * Returns true if the input is likely compressible or false if it cannot be determined.
	 * Note that a false return from this value does <i>not</i> imply the data is uncompressible.
	 * @param buf input buffer
	 * @param threshold percentage of bytes that must be successfully predicted to declare compressibility
	 * @return true if the input is likely compressible
	 */
	static boolean isLikelyCompressible(ByteBuffer buf, float threshold) {
		byte[] order1 = new byte[256]; // contains the last byte seen when the previous byte was a given value
		byte context = 0x0; // last byte seen
		int hits = 0; // number of times correctly predicted
		int total = buf.remaining(); // bytes in the input
		while (buf.hasRemaining()) {
			// get the prediction for the last byte
			byte prediction = order1[context & 0xFF];
			// get the actual buffer value
			byte input = buf.get();
			// did we pick correctly? score it
			if (prediction == input) hits++;
			// update the context prediction with the byte actually seen
			order1[context & 0xFF] = input;
			// shift context to the byte just seen
			context = input;
		}
		
		return ((float) hits / (float) total) >= threshold;
	}
}
