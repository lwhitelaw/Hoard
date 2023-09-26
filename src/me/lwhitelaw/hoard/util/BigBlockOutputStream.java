package me.lwhitelaw.hoard.util;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class BigBlockOutputStream extends OutputStream {
	public interface Output {
		byte[] writeBlock(ByteBuffer input);
	}
	
	// Header
	private static final int HEADER_SIZE = 12;
	// Offsets
	private static final int HEADER_OFFS_MAGIC = 0; // Magic value (should be HEADER_MAGIC)
	private static final int HEADER_OFFS_NUM_BLOCKS = 8; // Number of blocks as a signed int
	private static final int HEADER_OFFS_HASHLIST = 12; // End of header, list of hashes starts here
	// Magic values
	private static final long HEADER_MAGIC = 0x5355504552424C4BL; // "SUPERBLK", magic for all superblocks
	private static final int MAX_CHUNK_LIMIT = 67108863; // Maximum number of chunks. Calculated as (2^31-16) / 32 bytes and rounded down.
	private static final int HASH_SIZE = 32; // 256-bit hashes use 32 bytes.
	
	private Output writeInterface;
	private byte[] finalHash; // null if not yet written out, final block hash
	private ByteBuffer currentBlock = ByteBuffer.allocate(65536).order(ByteOrder.BIG_ENDIAN); // block where bytes go in current chunk
	private Chunker chunker = new Chunker(10,12); // sum of last 1024 bytes, try to cut at 4K bytes
	private List<byte[]> hashList = new ArrayList<byte[]>(); // list of all chunk hashes so far
	
	public BigBlockOutputStream(Output out) {
		writeInterface = out;
	}
	
	@Override
	public void write(int b) throws IOException {
		if (finalHash != null) throw new IllegalStateException("Stream closed");
		currentBlock.put((byte) b);
		// update moving sum
		chunker.update(b);
		// if the chunker signals a marker and there is at least 4KB in current block, write out block
		// if the current block is a full 65536 bytes, write out also
		if ((currentBlock.position() >= 4096 && chunker.isMarker()) || !currentBlock.hasRemaining()) {
			// full block, push it out to the interface
			pushBlock();
		}
	}
	
	@Override
	public void close() throws IOException {
		if (currentBlock.position() > 0) {
			// if data exists, get it out
			pushBlock();
		}
		// create a superblock for the hashes
		// first calculate the needed buffer size
		int buffersize = HEADER_SIZE + HASH_SIZE * hashList.size();
		// allocate it
		ByteBuffer superblock = ByteBuffer.allocate(buffersize).order(ByteOrder.BIG_ENDIAN);
		// fill it
		superblock.putLong(HEADER_MAGIC);
		superblock.putInt(hashList.size());
		for (byte[] hash : hashList) {
			superblock.put(hash);
		}
		// flip it and hand it to the interface
		superblock.flip();
		finalHash = writeInterface.writeBlock(superblock);
	}
	
	private void pushBlock() throws IOException {
		if (hashList.size() == MAX_CHUNK_LIMIT) {
			throw new IOException("cannot store any more chunks in the superblock");
		}
		currentBlock.flip();
		byte[] hash = writeInterface.writeBlock(currentBlock);
		hashList.add(hash);
		currentBlock.clear();
	}
	
	public byte[] getHash() {
		return finalHash;
	}
}
