package me.lwhitelaw.hoard;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.TreeMap;

import me.lwhitelaw.hoard.util.Buffers;


public final class PackfileWriter {
	private final TreeMap<BoxedHash,PackfileEntry> entries; // Keyed on hash, values are packfile entries to be inserted
	private final FileChannel file;
	private ByteBuffer compressionBuffer;
	
	public PackfileWriter(Path filePath) throws IOException {
		file = FileChannel.open(filePath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
		entries = new TreeMap<>();
		writeHeader(0,0); // Placeholder header
	}

	/**
	 * Return true if the writer is empty.
	 * @return true if the writer is empty.
	 */
	public boolean isEmpty() {
		return entries.isEmpty();
	}
	
	/**
	 * Return the number of blocks written to this packfile. This value will never exceed {@link Integer#MAX_VALUE}.
	 * @return the number of written blocks
	 */
	public int blockCount() {
		return entries.size();
	}

	/**
	 * Write the input into the packfile, if it is not present and return the hash of the data.
	 * @throws IOException if an I/O error occurs during writing
	 */
	public byte[] writeBlock(ByteBuffer input) throws IOException {
		if (blockCount() == Integer.MAX_VALUE) {
			throw new IOException("Cannot write any more blocks. Block count limit reached.");
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
		long payloadIndex = file.position(); // get position where file pointer is at, since data will be written here.
		// Get compression buffer ready
		prepareCompressionBufferWithSize(input.remaining());
		// Try compressing
		boolean compressed = Compression.compressHeuristically(0.2f, 1, input, compressionBuffer);
		// Get encoding based on whether compression succeeded
		long encoding = compressed? Format.ZLIB_ENCODING : Format.RAW_ENCODING;
		// Calculate encoded length from the encoded buffer
		compressionBuffer.flip();
		int encodedLength = compressionBuffer.remaining();
		// Move file position to end and write out the payload
		file.position(file.size());
		Buffers.writeFully(file, compressionBuffer);
		// Make block table entry and add it to the tree using the boxed hash key
		entries.put(boxed, new PackfileEntry(hash, encoding, length, encodedLength, payloadIndex));
		return hash;
	}
	
//	/**
//	 * Write the input into the packfile with the given hash, if it is not present and return the same hash.
//	 * There must be enough remaining capacity to write the entire input, even if the input is compressed
//	 * to a smaller size. The hash is assumed to be correctly precalculated using {@link Hashes#doHash(ByteBuffer)}.
//	 * <b>Failure to do so will potentially cause data corruption!</b>
//	 */
//	public byte[] writeBlockUnsafe(ByteBuffer input, byte[] hash) {
//		if (hash == null) throw new IllegalArgumentException("hash is null");
//		if (hash.length != 32) throw new IllegalArgumentException("hash must be 32 bytes long (256 bits)");
//		// Check if there's enough room to copy input
//		if (input.remaining() > dataArea.remaining()) {
//			// there isn't
//			throw new BufferOverflowException();
//		}
//		// Create "boxed" version for use in tree lookup
//		BoxedHash boxed = BoxedHash.valueOf(hash);
//		// Check if data exists, if so, do not write it.
//		if (entries.containsKey(boxed)) {
//			return hash;
//		}
//		// Get relevant data for the block table entry
//		int length = input.remaining(); // block length
//		int payloadIndex = dataArea.position(); // get position where data area is now, since this is where the payload will go.
//		// Try compressing
//		boolean compressed = Compression.compressHeuristically(0.2f, 1, input, dataArea);
//		// Get encoding based on whether compression succeeded
//		long encoding = compressed? Format.ZLIB_ENCODING : Format.RAW_ENCODING;
//		// Calculate encoded length from the current data area position and the payload index
//		int encodedLength = dataArea.position() - payloadIndex;
//		// Make block table entry and add it to the tree using the boxed hash key
//		entries.put(boxed, new PackfileEntry(hash, encoding, length, encodedLength, payloadIndex & 0x7FFFFFFFL));
//		return hash;
//	}
	
	public void padTo64Bytes(FileChannel channel) throws IOException {
		ByteBuffer buf = ByteBuffer.allocate(64).order(ByteOrder.BIG_ENDIAN);
		long neededPadding = Format.roundUp64(channel.size()) - channel.size();
		System.out.println("At " + channel.size() + ", writing " + neededPadding);
		while (neededPadding > 0) {
			buf.put((byte) 0);
			neededPadding--;
		}
		buf.flip();
		Buffers.writeFully(channel, buf);
	}
	
	public void close() throws IOException {
		// Move file position to end
		file.position(file.size());
		// Write out padding
		padTo64Bytes(file);
		// Get the blocktable start and length
		long blocktableStart = file.position();
		int blocktableLength = blockCount();
		// Write out the block table entries by traversing tree values in ascending order
		ByteBuffer ebuf = ByteBuffer.allocateDirect(Format.ENTRY_SIZE).order(ByteOrder.BIG_ENDIAN);
		for (PackfileEntry entry : entries.values()) {
			ebuf.clear();
			entry.toBuffer(ebuf);
			ebuf.flip();
			Buffers.writeFully(file, ebuf);
		}
		// Patch the header with the blocktable meta
		writeHeader(blocktableStart, blocktableLength);
		// Close file
		file.close();
	}
	
	private void prepareCompressionBufferWithSize(int size) {
		if (compressionBuffer == null || compressionBuffer.capacity() < size) {
			compressionBuffer = ByteBuffer.allocateDirect(size).order(ByteOrder.BIG_ENDIAN);
		}
		compressionBuffer.clear();
	}
	
	private void writeHeader(long blocktableStart, int blocktableLength) throws IOException {
		// Write out header
		ByteBuffer hbuf = ByteBuffer.allocateDirect(Format.HEADER_SIZE).order(ByteOrder.BIG_ENDIAN);
		hbuf.putLong(Format.HEADER_MAGIC); // Magic value
		hbuf.putLong(blocktableStart); // blocktable start
		hbuf.putInt(blocktableLength); // blocktable entries
		while (hbuf.hasRemaining()) hbuf.put((byte) 0); // Reserved
		hbuf.flip(); // buf fill -> drain
		Buffers.writeFileFully(file, hbuf, Format.HEADER_OFFS_MAGIC);
	}
}
