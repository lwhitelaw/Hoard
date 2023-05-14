package me.lwhitelaw.hoard.util;

import java.util.Arrays;

public class Chunker {
	private final int mod; // Modulus to slice by- if zero, marker here.
	private final byte[] ringbuffer; // Ring buffer for all n bytes recently seen. Starts zero-filled.
	private final int bufMask; // mask for ring buffer's length
	private int bufferIndex; // Index of next byte to be evicted and overwritten on update.
	private int sum; // Sum of all bytes in the buffer.
	
	public Chunker(int bufferSizePOT, int chunkModulusPOT) {
		mod = (1 << chunkModulusPOT) - 1;
		ringbuffer = new byte[1 << bufferSizePOT];
		bufMask = (1 << bufferSizePOT) - 1;
		bufferIndex = 0;
		sum = 0;
	}
	
	public void reset() {
		Arrays.fill(ringbuffer, (byte) 0);
		sum = 0;
		bufferIndex = 0;
	}

	public void update(byte b) {
		update(b & 0xFF);
	}
	
	public void update(int b) {
		b = b & 0xFF; // Constrain value by masking only the lower 8 bits
		int lastByte = ringbuffer[bufferIndex] & 0xFF; // Save the byte that is about to be evicted from the ringbuffer
		sum = sum + b - lastByte; // Update hash sum with the byte about to be inserted, subtracting byte about to be evicted
		ringbuffer[bufferIndex] = (byte) b; // Overwrite the evicted byte with the incoming byte
		bufferIndex = (bufferIndex + 1) & bufMask; // Move to next buffer position mod buffer size, shifting history by one
	}
	
	public boolean isMarker() {
		return ((sum & 0x7FFFFFFF) & mod) == 0; // Return zero if the sum is evenly divisible
	}
}
