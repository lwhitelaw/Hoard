package me.lwhitelaw.hoard;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

public final class PackfileWriter implements Repository {
	private List<PackfileEntry> entries;
	private ByteBuffer dataArea;
	
	public PackfileWriter(int dataAreaSize) {
		dataArea = ByteBuffer.allocate(dataAreaSize);
	}

	@Override
	public int hashSize() {
		return 32;
	}

	@Override
	public byte[] writeBlock(ByteBuffer data) {
		// Hash data
		byte[] hash = Hashes.doHash(data.duplicate());
		// Check if data exists
		return null;
	}

	@Override
	public ByteBuffer readBlock(byte[] hash) {
		// TODO Auto-generated method stub
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
