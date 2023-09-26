package me.lwhitelaw.hoard;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedList;
import java.util.TreeMap;

import me.lwhitelaw.hoard.util.Buffers;

/**
 * A writer for a Hoard packfile. A packfile may contain at most 2^63-1 unique blocks, attempting to write more will fail. Each block may have 2^31-1
 * bytes. Writes of identical blocks are coalesced within the Hoard packfile.
 * 
 * The packfile contents are buffered into memory and written out all at once. Therefore, enough memory should exist to hold all uncompressed blocks
 * before the packfile is written out.
 *
 * Instances of this class need external synchronisation.
 */
public final class PackfileWriter {
	private final TreeMap<BoxedHash,PackfileEntry> entries; // Keyed on hash, values are packfile entries to be inserted
	private final LinkedList<ByteBuffer> databufs; // Buffers to be written out.
	private long nextDataArea = 0; // Where in the data area the next data block will begin. Used for correctly constructing the packfile entries.
	private long numBlocks = 0;
	
	/**
	 * Create a packfile writer.
	 */
	public PackfileWriter() {
		entries = new TreeMap<>();
		databufs = new LinkedList<ByteBuffer>();
		nextDataArea = 0L;
		numBlocks = 0L;
	}

	/**
	 * Return true if the writer is empty.
	 * @return true if the writer is empty.
	 */
	public boolean isEmpty() {
		return entries.isEmpty();
	}
	
	/**
	 * Return the number of blocks written to this packfile. This value will never exceed {@link Long#MAX_VALUE}.
	 * @return the number of written blocks
	 */
	public long blockCount() {
		return numBlocks;
	}

	/**
	 * Write the input into the packfile, if it is not present and return the hash of the data.
	 */
	public byte[] writeBlock(ByteBuffer input) {
		if (blockCount() == Long.MAX_VALUE) {
			throw new IndexOutOfBoundsException("Cannot write any more blocks. Block count limit reached.");
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
		long payloadIndex = nextDataArea;
		// Get compression buffer ready
		ByteBuffer compressionBuffer = ByteBuffer.allocate(length).order(ByteOrder.BIG_ENDIAN);
		// Try compressing into it.
		boolean compressed = Compression.compressHeuristically(0.2f, 1, input, compressionBuffer);
		// Get encoding based on whether compression succeeded
		long encoding = compressed? Format.ZLIB_ENCODING : Format.RAW_ENCODING;
		// Calculate encoded length from the encoded buffer
		compressionBuffer.flip();
		int encodedLength = compressionBuffer.remaining();
		// Append buffer to the list and incrememt nextDataArea for bookkeeping where next data block goes.
		databufs.add(compressionBuffer);
		nextDataArea += encodedLength;
		// Make block table entry and add it to the tree using the boxed hash key
		entries.put(boxed, new PackfileEntry(hash, encoding, length, encodedLength, payloadIndex));
		numBlocks++;
		return hash;
	}
	
	/**
	 * Pad file to next 64 bytes by inserting zeroes.
	 * @param channel channel to write to
	 * @throws IOException if a write error occurs
	 */
	private static void padTo64Bytes(FileChannel channel) throws IOException {
		ByteBuffer buf = ByteBuffer.allocate(64).order(ByteOrder.BIG_ENDIAN);
		long neededPadding = Format.roundUp64(channel.size()) - channel.size();
//		System.out.println("At " + channel.size() + ", writing " + neededPadding);
		while (neededPadding > 0) {
			buf.put((byte) 0);
			neededPadding--;
		}
		buf.flip();
		Buffers.writeFully(channel, buf);
	}
	
	/**
	 * Dump the packfile, writing out the block table and data to the given file.
	 * The packfile writer cannot be used after this method is called.
	 * @throws IOException if a write error occurs.
	 */
	public void dump(Path path) throws IOException {
		// Open file
		FileChannel file = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
		// Calculate the starting location of the data area based on what we know.
		long dataAreaStart = Format.OFFS_BLOCKTABLE_START + Format.ENTRY_SIZE * numBlocks;
		// Write out the header with the blocktable meta
		writeHeader(numBlocks, dataAreaStart, file);
		// set file position after header
		file.position(Format.OFFS_BLOCKTABLE_START);
		// Write out the block table entries by traversing tree values in ascending order
		ByteBuffer ebuf = ByteBuffer.allocateDirect(Format.ENTRY_SIZE).order(ByteOrder.BIG_ENDIAN);
		for (PackfileEntry entry : entries.values()) {
			ebuf.clear();
			entry.toBuffer(ebuf);
			ebuf.flip();
			Buffers.writeFully(file, ebuf);
		}
		// Write out the block data
		for (ByteBuffer b : databufs) {
			Buffers.writeFully(file, b);
		}
		// Close file
		file.close();
	}
	
	/**
	 * Write out the header with the given blocktable information.
	 * @param blocktableLength The number of blocktable entries
	 * @throws IOException if an I/O error occurs during writing
	 */
	private void writeHeader(long blocktableLength, long dataAreaStart, FileChannel file) throws IOException {
		// Write out header
		ByteBuffer hbuf = ByteBuffer.allocateDirect(Format.HEADER_SIZE).order(ByteOrder.BIG_ENDIAN);
		hbuf.putLong(Format.HEADER_MAGIC); // Magic value
		hbuf.putLong(blocktableLength); // blocktable entries
		hbuf.putLong(dataAreaStart);
		while (hbuf.hasRemaining()) hbuf.put((byte) 0); // Reserved
		hbuf.flip(); // buf fill -> drain
		Buffers.writeFileFully(file, hbuf, Format.HEADER_OFFS_MAGIC);
	}
	
	/**
	 * Clear all data so this packfile writer can be reused.
	 */
	public void reset() {
		entries.clear();
		databufs.clear();
		numBlocks = 0L;
		nextDataArea = 0L;
	}
}
