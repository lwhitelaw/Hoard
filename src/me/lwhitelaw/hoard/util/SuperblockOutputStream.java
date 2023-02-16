package me.lwhitelaw.hoard.util;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

import me.lwhitelaw.hoard.RecoverableRepositoryException;
import me.lwhitelaw.hoard.Repository;

/**
 * An OutputStream that writes a series of variable-size blocks to a repository, with the hashes of these blocks coalesced into a
 * B-tree of pointer superblocks. Each data block is formed from chunking the stream using a moving sum creating variable-sized
 * blocks ranging from 4096 to 65535 bytes. Superblocks contain at most 1024 pointers to subblocks.
 * 
 * A superblock tree emitted by this class can store an unlimited amount of data, provided the underlying repository
 * is capable of storing all of the blocks required to represent it.
 * 
 * Upon closure of the stream, the hash to the superblock tree can be obtained.
 */
public class SuperblockOutputStream extends OutputStream {
	// leaf blocks go to level 0
	// once level 0 has 1024 blocks, all block hashes are compiled into a pointer block and put in level 1
	// level 0 is then cleared; same process repeats for level 1, 2, etc...
	// once stream is finished, lower levels have their blocks combined into upper levels repeatedly until one block remains
	//   even if this produces a pointer block pointing to only one leaf/pointer block from the lower level
	
	private final Repository repo; // the repository to which all blocks are written
	private final ByteBuffer[] currentSuperblocks; // buffers for up to 24 levels of superblocks yet to be written
	private final ByteBuffer currentBlock; // buffer for the current leaf block yet to be written
	private int maxSuperblockUsed = 0; // Maximum level of superblocks ever requested during writing; used for determining where to stop promotion
	private byte[] finalHash; // hash root of the superblock tree; null while the stream isn't closed
	
	// Header
	private static final int HEADER_SIZE = 12;
	// Offsets
	private static final int HEADER_OFFS_MAGIC = 0; // Magic value (should be HEADER_MAGIC)
	private static final int HEADER_OFFS_LEVEL = 8; // Level byte, up to 255. Level n blocks refer to superblocks of level n-1, level 0 blocks refer to data
	private static final int HEADER_OFFS_RESERVED = 9; // Reserved byte. Always zero.
	private static final int HEADER_OFFS_NUM_BLOCKS = 10; // Number of blocks as an unsigned short. Ranges 0 to 1024.
	private static final int HEADER_OFFS_HASHLIST = 12; // End of header, list of hashes starts here
	// Magic values
	private static final long HEADER_MAGIC = 0x5355504552424C4BL; // "SUPERBLK", magic for all superblocks
	private static final int MAX_BLOCKS_PER_LEVEL = 1024; // max number of blocks in a superblock
	private static final int MAX_LEVELS = 24; // Maximum levels the tree can stack to in this implementatione	epo
	/*
	 * Each data block can hold 4096 to 65535 bytes, so 2^12 to 2^16-1 bytes.
	 * Each superblock holds 1024 hashes, so 10 added to the exponent per superblock level.
	 * 24 levels is 240 added to the exponent, giving 2^240 + (2^12 to 2^16-1) = 2^252 to just under 2^256 bytes.
	 * That ought to be practically infinite for all intents and purposes.
	 */
	
	public SuperblockOutputStream(Repository repo) {
		this.repo = repo;
		currentBlock = ByteBuffer.allocate(65535);
		currentSuperblocks = new ByteBuffer[24];
	}

	@Override
	public void write(int b) throws IOException {
		currentBlock.put((byte) b);
		if (!currentBlock.hasRemaining()) {
			// full block, push it
			pushBlock();
		}
	}
	
	private void pushBlock() {
		// Write back data block
		currentBlock.flip(); // to drain
		byte[] hash = repo.writeBlock(currentBlock);
		currentBlock.clear();
		// Push into lowest superblock
		ByteBuffer superblock = getBlockListForLevel(0);
		putHashInSuperblock(superblock, hash);
		promoteFullBlocks();
	}
	
	// Handle promotions of full blocks once lower levels hit 1024
	private void promoteFullBlocks() {
		final int maxlevel = maxSuperblockUsed; // snapshot at the beginning to stop infinite looping
		for (int level = 0; level <= maxlevel; level++) {
			ByteBuffer superblock = getBlockListForLevel(level);
			if (getSuperblockNumBlocks(superblock) < 1024) return; // nothing to promote here
			// else promotion needed
			// write the superblock
			superblock.flip(); // to drain
			byte[] hash = repo.writeBlock(superblock);
			superblock.clear();
			// reinitialise superblock just written
			resetSuperblock(superblock, level);
			// push hash into next superblock level
			ByteBuffer upperSuperblock = getBlockListForLevel(level + 1); // note: raises maxSuperblockUsed
			putHashInSuperblock(upperSuperblock, hash);
			// if this push causes a full superblock, it'll be handled on the next loop
		}
	}
	
	// Consolidate blocks remaining on all levels into one hash
	private void consolidateBlocks() {
		final int maxlevel = maxSuperblockUsed; // snapshot at the beginning to stop infinite looping
		for (int level = 0; level <= maxlevel; level++) {
			ByteBuffer superblock = getBlockListForLevel(level);
			if (getSuperblockNumBlocks(superblock) == 0) {
				// an empty level does not need promotion
				continue;
			}
			// else promotion needed
			// write the superblock
			superblock.flip(); // to drain
			byte[] hash = repo.writeBlock(superblock);
			superblock.clear();
			// reinitialise
			resetSuperblock(superblock, level);
			// push hash into next superblock
			ByteBuffer upperSuperblock = getBlockListForLevel(level + 1); // note: raises maxSuperblockUsed
			putHashInSuperblock(upperSuperblock, hash);
			// this should let blocks "bubble up"
		}
	}
	
	private ByteBuffer getBlockListForLevel(int level) {
		if (level >= currentSuperblocks.length) {
			// Somehow accessed level 24... user would have written 2^252 bytes to see this!
			throw new RecoverableRepositoryException("too many blocks", null);
		}
		if (currentSuperblocks[level] == null) {
			currentSuperblocks[level] = ByteBuffer.allocate(65535);
			resetSuperblock(currentSuperblocks[level], level);
		}
		maxSuperblockUsed = Math.max(level, maxSuperblockUsed);
		return currentSuperblocks[level];
	}
	
	private void resetSuperblock(ByteBuffer superblock, int level) {
		// Get or create buffer for given level, then make sure it is cleared
		superblock.clear();
		// Create header
		superblock.putLong(HEADER_MAGIC);
		superblock.put((byte)(level & 0xFF)); // level 0 points to data blocks, all others do not
		superblock.put((byte)0x00); // reserved byte
		superblock.putShort((short)0); // number of hashes
		// position now where hashes will be written
	}
	
	private static void putHashInSuperblock(ByteBuffer superblock, byte[] hash) {
		// Increment hash count (these are absolute so position shouldn't move)
		int count = getSuperblockNumBlocks(superblock);
		if (count >= 1024) {
			throw new AssertionError("exceeded hash limit of 1024");
		}
		setSuperblockNumBlocks(superblock, count + 1);
		// now go back to end of buffer and write hash bytes
		superblock.put(hash);
	}
	
	private static int getSuperblockNumBlocks(ByteBuffer superblock) {
		return superblock.getShort(HEADER_OFFS_NUM_BLOCKS) & 0xFFFF;
	}
	
	private static void setSuperblockNumBlocks(ByteBuffer superblock, int value) {
		superblock.putShort(HEADER_OFFS_NUM_BLOCKS, (short) value);
	}
	
	@Override
	public void close() throws IOException {
		// handle the final block
		pushBlock();
	}
	
	public byte[] getHash() {
		return finalHash;
	}
}
