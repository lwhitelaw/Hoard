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
 * A superblock tree emitted by this class can store any amount of data up to the tree depth limit of 24, provided the underlying repository
 * is capable of storing all of the blocks required to represent it.
 * 
 * Upon closure of the stream, the hash to the superblock tree can be obtained.
 */
public class SuperblockOutputStream extends OutputStream {
	// leaf blocks go to level 0
	// once level 0 has 1024 blocks, all block hashes are compiled into a pointer block and put in level 1
	// level 0 is then cleared; same process repeats for level 1, 2, etc...
	// once stream is finished, lower levels have their blocks combined into upper levels
	// until reaching the top
	
	private final Repository repo; // the repository to which all blocks are written
	private final ByteBuffer[] currentSuperblocks; // buffers for up to 24 levels of superblocks yet to be written
	private final ByteBuffer currentBlock; // buffer for the current leaf block yet to be written
	private byte[] finalHash; // hash root of the superblock tree; null while the stream isn't closed
	private boolean nonempty; // latches true if the stream has at least a byte of data written
	private boolean treeFull; // latches true if the top level has 1024 blocks so no more data can be written without dropping it
	private Chunker chunker; // computes moving sum of bytes written to determine split points
	
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
	private static final int MAX_LEVELS = 24; // Maximum levels the tree can stack to in this implementation
	private static final int TOP_LEVEL = MAX_LEVELS - 1; // the highest level in the tree; if this hits 1024 no more data can be accepted
	/*
	 * Each data block can hold 4096 to 65535 bytes, so 2^12 to 2^16-1 bytes.
	 * Each superblock holds 1024 hashes, so 10 added to the exponent per superblock level.
	 * 24 levels is 240 added to the exponent, giving 2^240 + (2^12 to 2^16-1) = 2^252 to just under 2^256 bytes.
	 * That ought to be practically infinite for all intents and purposes.
	 */
	
	public SuperblockOutputStream(Repository repo) {
		this.repo = repo;
		currentBlock = ByteBuffer.allocate(65535).order(ByteOrder.BIG_ENDIAN);
		currentSuperblocks = new ByteBuffer[MAX_LEVELS];
		nonempty = false;
		treeFull = false;
		chunker = new Chunker(10,12); // sum of last 1024 bytes, try to cut at 4K bytes
	}

	@Override
	public void write(int b) {
		if (finalHash != null) throw new IllegalStateException("Stream closed");
		if (treeFull) throw new RecoverableRepositoryException("No more data can be written to this stream without truncation", null);
		currentBlock.put((byte) b);
		// update moving sum
		chunker.update(b);
		// if the chunker signals a marker and there is at least 4KB in current block, write out block
		// if the current block is a full 65535 bytes, write out also
		if ((currentBlock.position() >= 4096 && chunker.isMarker()) || !currentBlock.hasRemaining()) {
			// full block, push it
			pushBlock();
		}
		nonempty = true; // calling write at all by definition makes the stream not empty
	}
	
	// Push the current data block into superblock level 0
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
		int level = 0;
		while (level < TOP_LEVEL) {
			ByteBuffer superblock = getBlockListForLevel(level);
			if (getSuperblockNumBlocks(superblock) < MAX_BLOCKS_PER_LEVEL) return; // nothing to promote here
			// else promotion needed
			// write the superblock
			superblock.flip(); // to drain
			byte[] hash = repo.writeBlock(superblock);
			superblock.clear();
			// reinitialise superblock just written
			resetSuperblock(superblock, level);
			// push hash into next superblock level
			final int upperLevel = level + 1;
			ByteBuffer upperSuperblock = getBlockListForLevel(upperLevel);
			putHashInSuperblock(upperSuperblock, hash);
			// if this push causes a full superblock, it'll be handled on the next loop
			
			// check if the superblock we just pushed into is the last superblock level
			// if it is now full, we do not want to accept more data because it cannot be handled
			if (upperLevel == TOP_LEVEL && getSuperblockNumBlocks(upperSuperblock) == MAX_BLOCKS_PER_LEVEL) treeFull = true;
			level++;
		}
	}
	
	// Consolidate blocks remaining on all levels into one hash
	private void consolidateBlocks() {
		// if no data has been written, push an empty block forcibly, so level 0 has 1 block
		if (!nonempty) pushBlock();
		// walk the levels to determine what has blocks and what does not
		int maxLevel = 0; // max level on which blocks are present
		int numBlocks = 0; // total number of blocks present among all levels
		for (int i = 0; i < MAX_LEVELS; i++) {
			ByteBuffer superblock = currentSuperblocks[i];
			if (superblock != null) {
				int blocksInLevel = getSuperblockNumBlocks(superblock);
				if (blocksInLevel > 0) maxLevel = i;
				numBlocks += blocksInLevel;
			}
		}
		// now that data about the levels is known, there are four cases to handle.
		// case 1: numBlocks == 1, maxLevel == 0: One block in the stream
		//   - data blocks cannot exist alone, so coalesce into a level 1 block and write that
		// case 2: numBlocks == 1, maxLevel > 0: One block at a higher level, no blocks below it
		//   - take the hash of that one block and return that
		// case 3: numBlocks > 1, maxLevel == 0: Multiple blocks at level 0
		//   - coalesce upward and write
		// case 4: numBlocks > 1, maxLevel > 0: Multiple blocks at multiple levels
		//   - coalesce upward and write
		
		// handle cases 1 and 3
		if (maxLevel == 0) {
			ByteBuffer superblock = getBlockListForLevel(0);
			superblock.flip(); // to drain
			byte[] hash = repo.writeBlock(superblock);
			superblock.clear();
			finalHash = hash;
		} else if (numBlocks == 1) {
			// case 2
			ByteBuffer superblock = getBlockListForLevel(maxLevel);
			// extract the hash
			// calculate the size of the hash based on the write position in the superblock
			// minus the offset where the hash list should start... this should usually be 32 bytes
			// we know only one hash exists so this should work
			int hashSize = superblock.position() - HEADER_OFFS_HASHLIST;
			byte[] hash = new byte[hashSize];
			// copy from superblock
			superblock.get(HEADER_OFFS_HASHLIST, hash);
			finalHash = hash;
		} else {
			// case 4
			// start at level 0, coalesce upward until we reach maxLevel
			for (int level = 0; level < maxLevel; level++) {
				// get superblock for current level
				ByteBuffer superblock = getBlockListForLevel(level);
				if (getSuperblockNumBlocks(superblock) == 0) continue; // no blocks to promote here
				// otherwise write out block
				superblock.flip(); // to drain
				byte[] hash = repo.writeBlock(superblock);
				superblock.clear();
				// place hash in upper level
				final int upperLevel = level + 1;
				ByteBuffer upperSuperblock = getBlockListForLevel(upperLevel);
				putHashInSuperblock(upperSuperblock, hash);
			}
			// now write out final superblock in maxLevel
			ByteBuffer lastblock = getBlockListForLevel(maxLevel);
			lastblock.flip(); // to drain
			byte[] hash = repo.writeBlock(lastblock);
			lastblock.clear();
			finalHash = hash;
		}
	}
	
	private ByteBuffer getBlockListForLevel(int level) {
		if (level >= currentSuperblocks.length) {
			// Somehow accessed level 24... user would have written 2^252 bytes to see this!
			throw new RecoverableRepositoryException("too many blocks", null);
		}
		if (currentSuperblocks[level] == null) {
			currentSuperblocks[level] = ByteBuffer.allocate(65535).order(ByteOrder.BIG_ENDIAN);
			resetSuperblock(currentSuperblocks[level], level);
		}
		return currentSuperblocks[level];
	}
	
	private static void resetSuperblock(ByteBuffer superblock, int level) {
		// Make sure it is cleared
		superblock.clear();
		// Create header
		superblock.putLong(HEADER_MAGIC);
		superblock.put((byte)(level & 0xFF)); // level 0 points to data blocks, all others do not
		superblock.put((byte)0x00); // reserved byte
		superblock.putShort((short)0); // number of hashes
		// position now where hashes will be written
	}
	
	private static void putHashInSuperblock(ByteBuffer superblock, byte[] hash) {
		int position = superblock.position();
		// Increment hash count (these are absolute so position shouldn't move, save it just in case)
		int count = getSuperblockNumBlocks(superblock);
		if (count >= 1024) {
			throw new AssertionError("exceeded hash limit of 1024");
		}
		setSuperblockNumBlocks(superblock, count + 1);
		// now go back to end of buffer and write hash bytes
		superblock.position(position);
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
		// if the final hash has been assigned there is nothing to do
		if (finalHash != null) return;
		// push the current block if it has any data
		if (currentBlock.position() > 0) pushBlock();
		// consolidate blocks
		consolidateBlocks();
	}
	
	public byte[] getHash() {
		return finalHash;
	}
}
