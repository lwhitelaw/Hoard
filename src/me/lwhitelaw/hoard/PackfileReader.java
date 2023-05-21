package me.lwhitelaw.hoard;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class PackfileReader implements Repository {
	private final FileChannel file;
	private final int blocktableLength;
	
	public PackfileReader(Path filePath) throws IOException {
		file = FileChannel.open(filePath, StandardOpenOption.READ);
		blocktableLength = Format.checkHeader(file);
	}

	@Override
	public int hashSize() {
		return 32;
	}

	@Override
	public byte[] writeBlock(ByteBuffer data) {
		throw new RecoverableRepositoryException("Read-only", null);
	}

	@Override
	public ByteBuffer readBlock(byte[] hash) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() {
		try {
			file.close();
		} catch (IOException ex) {
			throw new RecoverableRepositoryException("I/O error on close; this seems rather unlikely", ex);
		}
	}

	@Override
	public void sync() {
		
	}
}
