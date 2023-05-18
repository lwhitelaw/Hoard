package me.lwhitelaw.hoard.util;

import java.nio.ByteBuffer;

public class Buffers {
	/**
	 * Copy data from the position to the limit into a new buffer of specified size. If the target size is smaller than the remaining
	 * elements, the data will be truncated.
	 * @param original buffer to copy from
	 * @param newSize new size, must not be negative
	 * @param allocateDirect if true, allocate a direct buffer
	 * @return a new buffer of specified size
	 */
	public static ByteBuffer reallocate(ByteBuffer original, int newSize, boolean allocateDirect) {
		if (newSize < 0) throw new IllegalArgumentException("size is negative");
		if (original == null) throw new NullPointerException("original buffer is null");
		ByteBuffer newBuffer = allocateDirect? ByteBuffer.allocateDirect(newSize) : ByteBuffer.allocate(newSize);
		if (newBuffer.remaining() < original.remaining()) {
			while (original.hasRemaining()) newBuffer.put(original.get());
		} else {
			newBuffer.put(original);
		}
		return newBuffer;
	}
	
	/**
	 * Like {@link ByteBuffer#put(ByteBuffer)}, but does not throw on underflow. Instead it puts as much data as will fit.
	 * @param target Target buffer to copy into
	 * @param src Source buffer to copy from
	 * @return The target buffer
	 */
	public static ByteBuffer putOptimistically(ByteBuffer target, ByteBuffer src) {
		if (target.remaining() < src.remaining()) {
			while (target.hasRemaining()) target.put(src.get());
		} else {
			target.put(src);
		}
		return target;
	}
}
