package me.lwhitelaw.hoard.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;

import me.lwhitelaw.hoard.Hashes;
import me.lwhitelaw.hoard.PackfileCollection;

public class SuperblockInputStream extends InputStream {
	private final PackfileCollection repo; // the repository from which blocks are read
	private final byte[] startingHash; // the original hash from which data is read
	private final ArrayDeque<ByteBuffer> currentSuperblocks; // stack of superblock buffers yet to be handled
	private ByteBuffer currentBlock; // buffer for the current leaf block to be read from
	
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

	public SuperblockInputStream(PackfileCollection repo, byte[] hash) {
		this.repo = repo;
		startingHash = new byte[hash.length];
		System.arraycopy(hash, 0, startingHash, 0, startingHash.length);
		currentSuperblocks = new ArrayDeque<>(24);
		currentBlock = null;
	}

	@Override
	public int read() throws IOException {
		while (currentBlock == null || !currentBlock.hasRemaining()) {
			// current block does not exist or has no more data to give
			// go to next block
			if (!nextBlock()) {
				// nextBlock says no more blocks left
				return -1;
			}
		}
		return currentBlock.get() & 0xFF;
	}
	
	private ByteBuffer readBlockOrThrow(byte[] hash) throws IOException {
		ByteBuffer block = repo.readBlock(hash);
		if (block == null) throw new IOException("Repo missing hash " + Hashes.hashToString(hash), null);
		block.order(ByteOrder.BIG_ENDIAN);
		return block;
	}
	
	private ByteBuffer readSuperblockOrThrow(byte[] hash) throws IOException {
		ByteBuffer block = readBlockOrThrow(hash);
		if (block.limit() < HEADER_SIZE) throw new IOException("Block " + Hashes.hashToString(hash) + " too short", null);
		if (block.getLong(HEADER_OFFS_MAGIC) != HEADER_MAGIC) {
			throw new IOException("Block " + Hashes.hashToString(hash) + " lacks magic value SUPERBLK", null);
		}
		return block;
	}
	
	private boolean nextBlock() throws IOException {
		// is stack empty? read first root superblock
		if (currentSuperblocks.isEmpty()) {
			// stack the top superblock based on starting hash
			ByteBuffer startingBlock = readSuperblockOrThrow(startingHash);
			// make sure buffer is pointing to hashlist, then stack it
			startingBlock.position(HEADER_OFFS_HASHLIST);
			currentSuperblocks.addFirst(startingBlock);
		}
		// pop any empty superblocks
		popEmptySuperblocks();
		// if at this point no blocks remain, nothing else to do
		if (currentSuperblocks.isEmpty()) {
			return false; // no data left
		}
		// given current tree top-of-stack, descend leftward to the leaves, stacking along the way
		descendTreeFromTopOfStack();
		// now get the leaf superblock
		ByteBuffer leafSuperblock = currentSuperblocks.peekFirst();
		// Sanity checking, make sure there's remaining hashes in the leaf block
		while (!leafSuperblock.hasRemaining()) {
			// if there isn't, go back up and descend down again
			// repeat this process until we get one or destack everything trying
			// pop any empty superblocks
			popEmptySuperblocks();
			// if at this point no blocks remain, nothing else to do
			if (currentSuperblocks.isEmpty()) {
				return false; // no data left
			}
			// given current tree top-of-stack, descend leftward to the leaves, stacking along the way
			descendTreeFromTopOfStack();
			// now try again
			leafSuperblock = currentSuperblocks.peekFirst();
		}
		// by this point we should have a superblock with at least one hash or destacked everything
		if (!leafSuperblock.hasRemaining()) throw new AssertionError("still did not get a block??");
		// get a hash frmo the superblock and set up the current data block
		byte[] dataHash = new byte[32];
		leafSuperblock.get(dataHash);
		currentBlock = readBlockOrThrow(dataHash);
		return true;
	}
	
	// Peek the top of stack and descend the tree from the current node
	// stacking superblocks along the way to the leaf.
	private void descendTreeFromTopOfStack() throws IOException {
		boolean seenLevelZero = false;
		while (!seenLevelZero) {
			// peek top of stack
			ByteBuffer superblock = currentSuperblocks.peekFirst();
			// If the top of stack is level zero...
			if (getSuperblockLevel(superblock) == 0) {
				// indicate we've seen this block so loop exits
				seenLevelZero = true;
			} else {
				// consume a hash from this superblock
				byte[] subHash = new byte[32];
				superblock.get(subHash);
				// read a superblock from the subhash and stack it
				ByteBuffer subblock = readSuperblockOrThrow(subHash);
				// point subblock's buffer to start of its hashlist
				subblock.position(HEADER_OFFS_HASHLIST);
				currentSuperblocks.addFirst(subblock);
			}
		}
	}
	
	// While the top of the stack has no more hashes, pop the stack
	// until there is a superblock with remaining hashes or the stack is emptied
	private void popEmptySuperblocks() {
		while (!currentSuperblocks.isEmpty()) {
			if (!currentSuperblocks.peekFirst().hasRemaining()) {
				currentSuperblocks.removeFirst();
			} else {
				return;
			}
		}
	}
	
	private static int getSuperblockNumBlocks(ByteBuffer superblock) {
		return superblock.getShort(HEADER_OFFS_NUM_BLOCKS) & 0xFFFF;
	}
	
	private static int getSuperblockLevel(ByteBuffer superblock) {
		return superblock.get(HEADER_OFFS_LEVEL) & 0xFF;
	}
}
