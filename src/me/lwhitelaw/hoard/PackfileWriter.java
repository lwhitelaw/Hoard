package me.lwhitelaw.hoard;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import me.lwhitelaw.hoard.util.Buffers;

public final class PackfileWriter implements Repository {
	private List<PackfileEntry> entries;
	private ByteBuffer dataArea;
	
	public PackfileWriter(int dataAreaSize) {
		dataArea = ByteBuffer.allocate(dataAreaSize);
		entries = new ArrayList<>();
	}

	@Override
	public int hashSize() {
		return 32;
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
	@Override
	public byte[] writeBlock(ByteBuffer input) {
		// Check if there's enough room to copy input
		if (input.remaining() > dataArea.remaining()) {
			// there isn't
			throw new BufferOverflowException();
		}
		// Hash data
		byte[] hash = Hashes.doHash(input.duplicate());
		// Check if data exists, if so, do not write it.
		// O(n) operation. This could be enhanced.
		if (entries.stream().anyMatch(entry -> Hashes.compare(hash, entry.getHash()) == 0)) {
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
		// Make block table entry and add it to the list
		entries.add(new PackfileEntry(hash, encoding, length, encodedLength, payloadIndex & 0x7FFFFFFFL));
		return hash;
	}

	@Override
	public ByteBuffer readBlock(byte[] hash) {
		return null;
	}

	@Override
	public void close() {
		
	}

	@Override
	public void sync() {
		
	}
	
	public void write(Path path) throws IOException {
		// Open file
		FileChannel file = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
		// Write out header
		ByteBuffer hbuf = ByteBuffer.allocate(Format.HEADER_SIZE).order(ByteOrder.BIG_ENDIAN);
		hbuf.putLong(Format.HEADER_MAGIC);
		hbuf.putInt(entries.size());
		hbuf.flip(); // buf fill -> drain
		Buffers.writeFully(file, hbuf);
		// Write out the block table entries
		ByteBuffer ebuf = ByteBuffer.allocate(Format.ENTRY_SIZE).order(ByteOrder.BIG_ENDIAN);
		for (PackfileEntry entry : entries) {
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
