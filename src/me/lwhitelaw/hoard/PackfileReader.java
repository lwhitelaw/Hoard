package me.lwhitelaw.hoard;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import me.lwhitelaw.hoard.util.Buffers;
import static me.lwhitelaw.hoard.Format.*;

public class PackfileReader implements Repository {
	private final FileChannel file;
	private final int blocktableLength;
	
	public PackfileReader(Path filePath) throws IOException {
		file = FileChannel.open(filePath, StandardOpenOption.READ);
		blocktableLength = checkHeader();
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
	
	// Utilities
	
	/**
	 * Check the header for a valid magic value and return the blocktable length.
	 * @return the length of the block table
	 * @throws IOException if there are problems reading the file, if the magic value is not correct, or if the block table length is invalid
	 */
	public int checkHeader() throws IOException {
		ByteBuffer hbuf = ByteBuffer.allocate(HEADER_SIZE).order(ByteOrder.BIG_ENDIAN);
		file.position(HEADER_OFFS_MAGIC);
		Buffers.readFully(file, hbuf);
		// check EOF
		if (hbuf.hasRemaining()) throw new IOException("Unexpected end of file");
		// check magic is valid
		if (hbuf.getLong(HEADER_OFFS_MAGIC) != HEADER_MAGIC) throw new IOException("Incorrect magic value");
		// extract and check blocktable length
		int length = hbuf.getInt(HEADER_OFFS_BLOCKTABLE_LENGTH);
		if (length < 0) throw new IOException("Blocktable length is invalid: " + length);
		return length;
	}
	
	/**
	 * Get the entry in the block table at the specified index.
	 * @param index the index into the block table to read
	 * @return the block table entry
	 * @throws IOException if there are problems reading the file or if the entry is malformed
	 */
	public PackfileEntry getBlocktableEntry(int index) throws IOException {
		ByteBuffer ebuf = ByteBuffer.allocate(ENTRY_SIZE).order(ByteOrder.BIG_ENDIAN);
		long filePosition = (long)ENTRY_SIZE * (long)index + (long)HEADER_SIZE;
		file.position(filePosition);
		Buffers.readFully(file, ebuf);
		// check EOF
		if (ebuf.hasRemaining()) throw new IOException("Unexpected end of file");
		// create object
		// PackfileEntry will do its own checks
		try {
			ebuf.flip();
			return PackfileEntry.fromBuffer(ebuf);
		} catch (IllegalArgumentException ex) {
			throw new IOException(ex.getMessage());
		}
	}
}
