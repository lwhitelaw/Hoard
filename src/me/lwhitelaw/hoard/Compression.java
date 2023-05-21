package me.lwhitelaw.hoard;

import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import me.lwhitelaw.hoard.util.Buffers;

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
		if (!buf.hasRemaining()) return false;
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
	
	/**
	 * Compress the input buffer's data into the output buffer, consuming the input buffer's data.
	 * If there is not enough space to hold the output, this method will return <code>false</code>.
	 * In this case, the input buffer will only be partially consumed and the output buffer
	 * filled with the compressed data that did fit. <code>false</code> will also be returned
	 * if ZLIB expands the input.
	 * @param level the ZLIB compression level to use, from 0-9
	 * @param input input data to compress
	 * @param output output buffer for compressed data
	 * @return true on success, false on failure due to lack of space or expansion.
	 */
	private static boolean compress(int level, ByteBuffer input, ByteBuffer output) {
		int inputSize = input.remaining();
		int outputStart = output.position();
		Deflater deflater = new Deflater(level);
		try {
			deflater.setInput(input);
			deflater.finish();
			while (!deflater.finished()) {
				if (!output.hasRemaining()) {
					// out of space! Unlikely unless the input is really incompressible
					return false;
				}
				deflater.deflate(output);
			}
		} finally {
			deflater.end();
		}
		// compression succeeded, but is it actually smaller?
		if (inputSize < output.position()-outputStart) {
			return false; // it isn't, so fail
		}
		return true;
	}
	
	/**
	 * Compress the input buffer's data into the output buffer, consuming the input buffer's data if
	 * the data is detected as compressible based on the given threshold.
	 * If there is not enough space to hold the output, the data will be copied as-is.
	 * The return value indicates whether the data was compressed.
	 * @param level the ZLIB compression level to use, from 0-9
	 * @param input input data to compress
	 * @param output output buffer for compressed data
	 * @return true if the data is compressed, or false if copied raw
	 */
	public static boolean compressHeuristically(float threshold, int level, ByteBuffer input, ByteBuffer output) {
		// Ensure the output buffer will always have enough room to hold the input if copied raw.
		if (input.remaining() > output.remaining()) {
			throw new IllegalStateException(String.format("Not enough room for a raw copy. Input remaining: %d, output remaining: %d",input.remaining(),output.remaining()));
		}
		// Determine if the data *should* be compressed. If level is zero, don't bother trying.
		boolean shouldTryCompress = level != 0 && isLikelyCompressible(input.duplicate(), threshold);
		if (!shouldTryCompress) {
			// Don't try compressing, copy as-is and return.
			output.put(input);
			return false;
		} else {
			// Save the buffer states in case we have to restart the operation.
			// Mark can't be saved, but OpenJDK states mark is not discarded unless position/limit < mark.
			// This invariant will not be violated so that isn't a problem.
			int inputPosition = input.position();
			int inputLimit = input.limit();
			int outputPosition = output.position();
			int outputLimit = output.limit();
			// Attempt compressing the data.
			boolean success = compress(level, input, output);
			// If compression succeeds, our job here is done. Return true.
			if (success) return true;
			// Compression failed. Rewind the state and copy as-is.
			input.limit(inputLimit).position(inputPosition);
			output.limit(outputLimit).position(outputPosition);
			output.put(input);
			return false;
		}
	}
	
	/**
	 * Decompress the input buffer's data into the output buffer, consuming the input buffer's data.
	 * If there is not enough space to hold the output, this method will return <code>false</code>.
	 * In this case, the input buffer will only be partially consumed and the output buffer
	 * filled with the decompressed data that did fit. DataFormatException will be thrown if for some
	 * reason the compressed input is not in valid ZLIB format.
	 * @param input input data to decompress
	 * @param output output buffer for decompressed data
	 * @return true on success, false on failure due to lack of space.
	 * @throws DataFormatException if the input data is malformed
	 */
	private static boolean decompress(ByteBuffer input, ByteBuffer output) throws DataFormatException {
		Inflater inflater = new Inflater();
		try {
			inflater.setInput(input);
			while (!inflater.finished()) {
				if (!output.hasRemaining()) {
					// out of space! likely if the compression ratio is unexpectedly high
					return false;
				}
				inflater.inflate(output);
			}
		} finally {
			inflater.end();
		}
		return true;
	}
}
