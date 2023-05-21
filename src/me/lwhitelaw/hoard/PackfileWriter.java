package me.lwhitelaw.hoard;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

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

	@Override
	public byte[] writeBlock(ByteBuffer input) {
		// Hash data
		byte[] hash = Hashes.doHash(input.duplicate());
		// Check if data exists, if so, do not write it.
		
		
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
		// Open file
		FileChannel file = FileChannel.open(Paths.get("./block"), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
		// Write out header and data
	}

	@Override
	public void sync() {
		
	}
}
