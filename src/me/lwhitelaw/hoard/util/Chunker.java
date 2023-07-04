package me.lwhitelaw.hoard.util;

import java.util.Arrays;

/**
 * A content-based slicer based on the moving sum of the last 2^n bytes provided and a chunk modulus m. A chunk boundary is declared when this
 * moving sum mod m equals zero. It is up to the user of the class to choose to act on chunk boundary markers.
 * Recommended values are n = 10, m = 12, for a buffer of 1024 bytes and a chunk modulus of 4096, which attempts to chunk data in sizes
 * close to 4 KB.
 *
 */
public class Chunker {
	private final int mod; // Modulus to slice by- if zero, marker here.
	private final byte[] ringbuffer; // Ring buffer for all n bytes recently seen. Starts zero-filled.
	private final int bufMask; // mask for ring buffer's length
	private int bufferIndex; // Index of next byte to be evicted and overwritten on update.
	private int sum; // Sum of all bytes in the buffer.
	
	/**
	 * Construct a chunker with the specified parameters, both expressed as power-of-two exponents.
	 * @param bufferSizePOT The size of the ring buffer and the number of bytes to sum, as a power of 2
	 * @param chunkModulusPOT The modulus of the sum at which a chunk will be declared if that value is zero, as a power of 2
	 */
	public Chunker(int bufferSizePOT, int chunkModulusPOT) {
		if (bufferSizePOT < 1 || bufferSizePOT > 31) throw new IllegalArgumentException("bad buffer size");
		if (chunkModulusPOT < 1 || chunkModulusPOT > 31) throw new IllegalArgumentException("bad chunk modulus");
		mod = (1 << chunkModulusPOT) - 1;
		ringbuffer = new byte[1 << bufferSizePOT];
		bufMask = (1 << bufferSizePOT) - 1;
		bufferIndex = 0;
		sum = 0;
	}
	
	/**
	 * Reset this chunker.
	 */
	public void reset() {
		Arrays.fill(ringbuffer, (byte) 0);
		sum = 0;
		bufferIndex = 0;
	}

	/**
	 * Insert a byte into the chunker.
	 * @param b The byte to insert
	 */
	public void update(byte b) {
		update(b & 0xFF);
	}
	
	/**
	 * Insert a byte into the chunker. The upper 24 bits in the int are ignored.
	 * @param b The byte to insert
	 */
	public void update(int b) {
		b = b & 0xFF; // Constrain value by masking only the lower 8 bits
		int lastByte = ringbuffer[bufferIndex] & 0xFF; // Save the byte that is about to be evicted from the ringbuffer
		sum = sum + b - lastByte; // Update hash sum with the byte about to be inserted, subtracting byte about to be evicted
		ringbuffer[bufferIndex] = (byte) b; // Overwrite the evicted byte with the incoming byte
		bufferIndex = (bufferIndex + 1) & bufMask; // Move to next buffer position mod buffer size, shifting history by one
	}
	
	/**
	 * Return true if the sum mod the chunk modulus is zero, and therefore a chunk boundary.
	 * @return true if a chunk boundary is present after the data thus far
	 */
	public boolean isMarker() {
		return ((sum & 0x7FFFFFFF) & mod) == 0; // Return zero if the sum is evenly divisible
	}
}
