package me.lwhitelaw.hoard.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import me.lwhitelaw.hoard.Hashes;

public class BigBlockInputStream extends InputStream {
	// Header
	private static final int HEADER_SIZE = 12;
	// Offsets
	private static final int HEADER_OFFS_MAGIC = 0; // Magic value (should be HEADER_MAGIC)
	private static final int HEADER_OFFS_NUM_BLOCKS = 8; // Number of blocks as a signed int
	private static final int HEADER_OFFS_HASHLIST = 12; // End of header, list of hashes starts here
	// Magic values
	private static final long HEADER_MAGIC = 0x5355504552424C4BL; // "SUPERBLK", magic for all superblocks
	private static final int HASH_SIZE = 32; // 256-bit hashes use 32 bytes.

	private BlockInput readInterface;
	private ByteBuffer superblockData;
	private ByteBuffer currentBlock;
	
	public BigBlockInputStream(BlockInput in, byte[] hash) throws IOException {
		readInterface = in;
		// read in superblock and verify
		superblockData = readInterface.readBlock(hash);
		verifySuperblock();
		// move position off to hash list start to prepare reading
		superblockData.position(HEADER_OFFS_HASHLIST);
	}

	private void verifySuperblock() throws IOException {
		Checks.checkSufficientSize(superblockData.remaining(), HEADER_SIZE, "header");
		Checks.checkExpectedValue(superblockData.getLong(HEADER_OFFS_MAGIC), HEADER_MAGIC, "magic");
		Checks.checkPositive(superblockData.getInt(HEADER_OFFS_NUM_BLOCKS), "num blocks");
		Checks.checkExpectedValue(superblockData.remaining(), HEADER_SIZE + superblockData.getInt(HEADER_OFFS_NUM_BLOCKS) * HASH_SIZE, "superblock size");
//		// Verify block is large enough to even have the header
//		if (superblockData.remaining() < HEADER_SIZE) throw new IOException("Superblock too small");
//		// Verify the header magic number is there
//		if (superblockData.getLong(HEADER_OFFS_MAGIC) != HEADER_MAGIC) throw new IOException("Block lacks magic value SUPERBLK");
//		// Verify the block count is not negative
//		if (superblockData.getInt(HEADER_OFFS_NUM_BLOCKS) < 0) throw new IOException("Corrupted block count");
//		// Verify the amount of data matches the expected size of the superblock
//		if (superblockData.remaining() != (HEADER_SIZE + superblockData.getInt(HEADER_OFFS_NUM_BLOCKS) * HASH_SIZE)) throw new IOException("The superblock is not the expected size");
	}

	@Override
	public int read() throws IOException {
		if (!isBlockReady()) return -1; // get the next block set up if needed
		// read a byte from current block
		return currentBlock.get() & 0xFF;
	}
	
	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		if (len <= 0) return 0; // read nothing if there is nothing to read
		if (!isBlockReady()) return -1; // get the next block set up; if we can't, declare end of stream
		// Setup variables
		int needed = len; // number of bytes we need to transfer
		int copyPointer = off; // index into b where copy ops will happen
		int transferred = 0; // number of bytes transferred
		
		// While b needs more data...
		while (needed > 0) {
			// Check again to make sure there is data, return if EOS is hit (only if a copy happened before)
			if (!isBlockReady()) return transferred;
			// Copy as much data as we can from the block
			int remainingInBlock = currentBlock.remaining();
			// If we need more data than this block has, copy only as much data as the block has
			int amountToCopy = Math.min(needed, remainingInBlock);
			currentBlock.get(b, copyPointer, amountToCopy);
			copyPointer += amountToCopy;
			transferred += amountToCopy;
			needed -= amountToCopy;
//			System.err.println("Copied " + amountToCopy);
		}
		// Return number of bytes transferred
		return transferred;
	}
	
	private boolean isBlockReady() throws IOException {
		while (currentBlock == null || !currentBlock.hasRemaining()) {
			// Current block does not exist/is empty... fill it
			if (!superblockData.hasRemaining()) {
				// No more hashes in the superblock to read from
				// End of stream
				return false;
			}
			// Read a hash and get the block
			byte[] hash = new byte[HASH_SIZE];
			superblockData.get(hash);
			currentBlock = readInterface.readBlock(hash);
			// if null that block is missing, stream is possibly corrupt/backing stores not available
			if (currentBlock == null) throw new IOException("Block for hash " + Hashes.hashToString(hash) + " is missing");
			// if currentBlock is zero-size (shouldn't happen, but is legal format) loop again to try to get another block
		}
		// current block is ready to read from
		return true;
	}
	
	@Override
	public void close() {
		// Doesn't need to do anything
	}
}
