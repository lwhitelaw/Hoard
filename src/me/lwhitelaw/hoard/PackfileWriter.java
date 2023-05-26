package me.lwhitelaw.hoard;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.TreeMap;

import me.lwhitelaw.hoard.util.Buffers;

public final class PackfileWriter {
	private final TreeMap<BoxedHash,PackfileEntry> entries; // Keyed on hash, values are packfile entries to be inserted
	private final ByteBuffer dataArea;
	
	public PackfileWriter(int dataAreaSize) {
		dataArea = ByteBuffer.allocateDirect(dataAreaSize);
		entries = new TreeMap<>();
	}
	
	/**
	 * Clear all data in this packfile writer.
	 */
	public void reset() {
		entries.clear();
		dataArea.clear();
	}
	
	/**
	 * Return true if the writer is empty.
	 * @return true if the writer is empty.
	 */
	public boolean isEmpty() {
		return entries.isEmpty();
	}
	
	/**
	 * Return the amount of bytes that can be provided to the packfile writer before the data area buffer is full.
	 * @return the amount of space left in the data area buffer
	 */
	public int remainingCapacity() {
		return dataArea.remaining();
	}

	/**
	 * Write the input into the packfile, if it is not present and return the hash of the data.
	 * There must be enough remaining capacity to write the entire input, even if the input is compressed
	 * to a smaller size.
	 */
	public byte[] writeBlock(ByteBuffer input) {
		// Check if there's enough room to copy input
		if (input.remaining() > dataArea.remaining()) {
			// there isn't
			throw new BufferOverflowException();
		}
		// Hash data
		byte[] hash = Hashes.doHash(input.duplicate());
		// Create "boxed" version for use in tree lookup
		BoxedHash boxed = BoxedHash.valueOf(hash);
		// Check if data exists, if so, do not write it.
		if (entries.containsKey(boxed)) {
			return hash;
		}
		// Get relevant data for the block table entry
		int length = input.remaining(); // block length
		int payloadIndex = dataArea.position(); // get position where data area is now, since this is where the payload will go.
		// Try compressing
		boolean compressed = Compression.compressHeuristically(0.2f, 1, input, dataArea);
		// Get encoding based on whether compression succeeded
		long encoding = compressed? Format.ZLIB_ENCODING : Format.RAW_ENCODING;
		// Calculate encoded length from the current data area position and the payload index
		int encodedLength = dataArea.position() - payloadIndex;
		// Make block table entry and add it to the tree using the boxed hash key
		entries.put(boxed, new PackfileEntry(hash, encoding, length, encodedLength, payloadIndex & 0x7FFFFFFFL));
		return hash;
	}
	
	/**
	 * Write the input into the packfile with the given hash, if it is not present and return the same hash.
	 * There must be enough remaining capacity to write the entire input, even if the input is compressed
	 * to a smaller size. The hash is assumed to be correctly precalculated using {@link Hashes#doHash(ByteBuffer)}.
	 * <b>Failure to do so will potentially cause data corruption!</b>
	 */
	public byte[] writeBlockUnsafe(ByteBuffer input, byte[] hash) {
		if (hash == null) throw new IllegalArgumentException("hash is null");
		if (hash.length != 32) throw new IllegalArgumentException("hash must be 32 bytes long (256 bits)");
		// Check if there's enough room to copy input
		if (input.remaining() > dataArea.remaining()) {
			// there isn't
			throw new BufferOverflowException();
		}
		// Create "boxed" version for use in tree lookup
		BoxedHash boxed = BoxedHash.valueOf(hash);
		// Check if data exists, if so, do not write it.
		if (entries.containsKey(boxed)) {
			return hash;
		}
		// Get relevant data for the block table entry
		int length = input.remaining(); // block length
		int payloadIndex = dataArea.position(); // get position where data area is now, since this is where the payload will go.
		// Try compressing
		boolean compressed = Compression.compressHeuristically(0.2f, 1, input, dataArea);
		// Get encoding based on whether compression succeeded
		long encoding = compressed? Format.ZLIB_ENCODING : Format.RAW_ENCODING;
		// Calculate encoded length from the current data area position and the payload index
		int encodedLength = dataArea.position() - payloadIndex;
		// Make block table entry and add it to the tree using the boxed hash key
		entries.put(boxed, new PackfileEntry(hash, encoding, length, encodedLength, payloadIndex & 0x7FFFFFFFL));
		return hash;
	}
	
	public void write(Path path) throws IOException {
		// Open file
		FileChannel file = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
		// Write out header
		ByteBuffer hbuf = ByteBuffer.allocateDirect(Format.HEADER_SIZE).order(ByteOrder.BIG_ENDIAN);
		hbuf.putLong(Format.HEADER_MAGIC);
		hbuf.putInt(entries.size());
		hbuf.flip(); // buf fill -> drain
		Buffers.writeFully(file, hbuf);
		// Write out the block table entries by traversing tree values in ascending order
		ByteBuffer ebuf = ByteBuffer.allocateDirect(Format.ENTRY_SIZE).order(ByteOrder.BIG_ENDIAN);
		for (PackfileEntry entry : entries.values()) {
			ebuf.clear();
			entry.toBuffer(ebuf);
			ebuf.flip();
			Buffers.writeFully(file, ebuf);
		}
		// Write out the data area
		dataArea.flip();
		Buffers.writeFully(file, dataArea);
		// Close file
		file.close();
	}
}
