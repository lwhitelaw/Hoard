package me.lwhitelaw.hoard.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

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
	
	/**
	 * Fully write this buffer to the channel, repeating until the buffer is drained.
	 * @param chan Channel to write to
	 * @param buf Buffer to drain
	 * @return the number of bytes written, which will be the number of bytes remaining
	 * @throws IOException if an I/O error occurs
	 */
	public static int writeFully(WritableByteChannel chan, ByteBuffer buf) throws IOException {
		int bytes = 0;
		while (buf.hasRemaining()) {
			bytes += chan.write(buf);
		}
		return bytes;
	}
	
	/**
	 * Fully read data into this buffer from the channel, repeating until the buffer is filled or
	 * end of stream is signalled.
	 * @param chan Channel to read from
	 * @param buf Buffer to fill
	 * @return the number of bytes read
	 * @throws IOException if an I/O error occurs
	 */
	public static int readFully(ReadableByteChannel chan, ByteBuffer buf) throws IOException {
		int bytes = 0;
		while (buf.hasRemaining()) {
			int readResult = chan.read(buf);
			if (readResult == -1) {
				return bytes;
			}
			bytes += readResult;
		}
		return bytes;
	}
	
	/**
	 * Fully write this buffer to the file at the given position, repeating until the buffer is drained.
	 * @param file File channel to write to
	 * @param buf Buffer to drain
	 * @param startLocation where in the file to start writing
	 * @return the number of bytes written, which will be the number of bytes remaining
	 * @throws IOException if an I/O error occurs
	 */
	public static int writeFileFully(FileChannel file, ByteBuffer buf, long startLocation) throws IOException {
		int bytes = 0;
		while (buf.hasRemaining()) {
			bytes += file.write(buf,startLocation + bytes);
		}
		return bytes;
	}
	
	/**
	 * Fully read data into this buffer from the file at the given position, repeating until the buffer is
	 * filled or end of stream is signalled.
	 * @param file File channel to read from
	 * @param buf Buffer to fill
	 * @param startLocation where in the file to start reading
	 * @return the number of bytes read
	 * @throws IOException if an I/O error occurs
	 */
	public static int readFileFully(FileChannel file, ByteBuffer buf, long startLocation) throws IOException {
		int bytes = 0;
		while (buf.hasRemaining()) {
			int readResult = file.read(buf,startLocation + bytes);
			if (readResult == -1) {
				return bytes;
			}
			bytes += readResult;
		}
		return bytes;
	}
}
