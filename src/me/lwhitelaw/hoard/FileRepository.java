package me.lwhitelaw.hoard;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import me.lwhitelaw.hoard.RepositoryException.Reason;

import static java.nio.file.StandardOpenOption.*;

/**
 * A file-based repository for content-addressed blocks of data. All blocks are stored in an append-only file
 * referenced by an in-memory index built on creation. Blocks are compressed with ZLIB if doing so is advantageous.
 * Data is kept consistent using "fsync markers" indicating where valid data ends in case of a crash or power loss.
 * Instances of this class are safe for use by multiple threads, however, operations do not proceed concurrently.
 * This repository uses SHA3-256 hashes.
 */
public class FileRepository implements Repository {
	/*
	Block format
	{
		0 "BLOCKHDR"           // Magic number (8 bytes)
		8 byte hash[32];       // 256-bit SHA3-256 hash of the block to follow
		40 byte encoding[4];   // compression mode: "\0\0\0\0" (uncompressed),"ZLIB" (zlib)
		                       //   other values may be present in later versions
		                       //   encodings that are not understood by the implementation are to be treated as if they were
		                       //   not present.
		44 ushort length;      // unsigned 16-bit raw length of the data following
		46 ushort elength;     // unsigned 16-bit encoded length of the data following
		48 byte data[elength]; // The payload data
	}
	fsync marker
	{
		0 "FSYNCEND"           // Magic number (8 bytes)
	}
	
	A block file consists simply of concatenating any number of the above structures in combination, as long as the file
	ends with an fsync marker. If a crash occurs and the fsync marker is not written to finalise any writes, upon
	the next open, any data after the last fsync marker in the file is ignored and will be truncated if possible to enforce
	consistency.
	
	Other implementations should at least support raw and ZLIB encoding; otherwise blocks in unknown encodings should
	not be indexed and treated as if the block was not present in the file, even if it means the block will be written more
	than once in the file using multiple encodings.
	*/
	
	private static final int HEADER_SIZE = 48; // size of the header in bytes
	// Offsets into the header
	private static final int HEADER_OFFS_MAGIC = 0; // Magic value (should be HEADER_MAGIC)
	private static final int HEADER_OFFS_HASH = 8; // SHA3-256 hash, 32 bytes
	private static final int HEADER_OFFS_ENCODING = 40; // Compression encoding, 4 bytes
	private static final int HEADER_OFFS_LENGTH = 44; // Payload size, unsigned short
	private static final int HEADER_OFFS_ELENGTH = 46; // Encoded payload size, unsigned short
	private static final int HEADER_OFFS_PAYLOAD = 48; // End of header, payload data starts here
	// Magic values
	private static final long FSYNC_END = 0x4653594E43454E44L; // "FSYNCEND", marks an fsync call; all data before assumed committed
	private static final long HEADER_MAGIC = 0x424C4F434B484452L; // "BLOCKHDR", start of a block
	private static final int RAW_ENCODING = 0x00000000; // raw encoded payload
	private static final int ZLIB_ENCODING = 0x5A4C4942; // "ZLIB", zlib encoded payload
	// Debug
	private static final boolean FORCE_RAW = false; // if true, never compress
	
	private final ReentrantLock lock; // operation lock
	private final ByteTrie<BlockLocation> index; // index organising hashes to offsets in file that contain their blocks
	private final Path blocksPath; // path to the file where blocks are stored
	private final FileChannel blocksFile; // the file where blocks are stored
	private final boolean readOnly; // if true, writing not possible
	private long lastFsyncEndOffset; // file offset where last fsync mark was written; data after is ignored and new writes start here
	
	/**
	 * Access a block repository on the specified file. If <code>writable</code> is <code>true</code>, the repository
	 * will be opened for writing, and the file created if needed. Otherwise, the repository is read-only. If the file
	 * has corrupted data and the repository is opened for writing, the corrupted data will be truncated up to the last
	 * fsync marker.
	 * @param path file path to the repository file
	 * @param writable true if writing should be allowed
	 * @throws RepositoryException if SHA3-256 is not supported by the JDK in use, or if an error occurs when opening the repository.
	 */
	public FileRepository(Path path, boolean writable) {
		try {
			MessageDigest.getInstance("SHA3-256"); // should throw if not present
		} catch (NoSuchAlgorithmException ex) {
			throw new RepositoryException("This JDK does not support SHA3-256 hashing", ex, Reason.ALGORITHM_NOT_SUPPORTED);
		}
		lock = new ReentrantLock();
		index = new ByteTrie<>();
		readOnly = !writable;
		blocksPath = path;
		// check writable and open file depending on mode
		try {
			if (writable) {
				blocksFile = FileChannel.open(path,CREATE,READ,WRITE);
			} else {
				blocksFile = FileChannel.open(path,READ);
			}
			// parse blocks and build the index
			initIndex(false);
			// if we are writable, truncate the file after the last fsync marker
			if (!readOnly) {
				blocksFile.truncate(lastFsyncEndOffset);
				blocksFile.force(false);
			}
		} catch (IOException ex) {
			throw new RepositoryException("Failed to open repository", ex, guessErrorReason(false));
		}
	}
	
	/**
	 * Returns 32, the size in bytes of a SHA3-256 hash.
	 */
	@Override
	public int hashSize() {
		return 32;
	}

	/**
	 * Close the repository. If the repository is writable,
	 * written data will be committed to non-volatile storage if needed.
	 * @throws RepositoryException if closing fails
	 */
	@Override
	public void close() {
		lock.lock();
		try {
			// Only if the file is open...
			if (blocksFile.isOpen()) {
				// Sync so the marker is written out, if the file is writable
				if (!readOnly) sync();
				// Close file
				closeFile();
			}
		} finally {
			lock.unlock();
		}
	}
	
	/**
	 * Ensure any written data is persisted to non-volatile storage. This method blocks until
	 * that is done. An "fsync marker" is written, then fsync is called to persist writes to disk.
	 * @throws RepositoryException if persistence fails
	 */
	public void sync() {
		checkOpenAndWritable();
		lock.lock();
		try {
			try {
				// move to end of file
				blocksFile.position(blocksFile.size());
				// if no data is actually written, nothing to do
				if (blocksFile.position() == lastFsyncEndOffset) return;
				// write an fsync end marker
				ByteBuffer fsyncEnd = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.BIG_ENDIAN);
				fsyncEnd.putLong(FSYNC_END);
				fsyncEnd.flip();
				writeFully(blocksFile, fsyncEnd);
				// emit a fsync
				blocksFile.force(false);
				// update transaction pointer to new file position
				lastFsyncEndOffset = blocksFile.position();
			} catch (IOException ex) {
				// failure of any of the above is a sync failure
				closeFile();
				throw new RepositoryException("Sync failed", ex, guessErrorReason(true));
			}
		} finally {
			lock.unlock();
		}
	}
	
	/**
	 * Initialise the index by reading the file's headers, optionally verifying payloads have their expected hashes.
	 * @param verifyPayloads if true, verify payloads have the hashes they should have
	 * @throws IOException if an IO error occurs
	 */
	private void initIndex(boolean verifyPayloads) throws IOException {
		// List of pending block locations before a fsync marker is seen
		List<BlockLocation> blocksBeforeSync = new ArrayList<>();
		// start at beginning
		blocksFile.position(0);
		// header buffer
		ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE).order(ByteOrder.BIG_ENDIAN);
		// keep reading headers until out of data
		while (blocksFile.position() < blocksFile.size()) {
			long startPosition = blocksFile.position(); // where the read started
			header.clear(); // prepare buffer for read
			int bytesRead = readFully(blocksFile, header);
			header.flip(); // "filling" -> "draining"; mostly a formality because of absolute addressing
			if (bytesRead < 8) {
				return; // not enough data to determine the magic
			} else if (bytesRead >= 8 && header.getLong(0) == FSYNC_END) {
				// it's an fsync marker
				// flush the blocks list and add them to index
				for (BlockLocation blk : blocksBeforeSync) {
					index.put(blk.hash(), blk);
				}
				blocksBeforeSync.clear();
				// set the end offset
				lastFsyncEndOffset = startPosition + 8;
				// jump 8 bytes
				blocksFile.position(startPosition + 8);
			} else if (bytesRead == 48 && header.getLong(0) == HEADER_MAGIC) {
				// it's a BLOCKHDR header
				// grab the hash
				byte[] hash = new byte[32];
				header.get(HEADER_OFFS_HASH, hash);
				// grab the encoding and lengths
				int encoding = header.getInt(HEADER_OFFS_ENCODING);
				int length = header.getShort(HEADER_OFFS_LENGTH) & 0xFFFF;
				int encodedLength = header.getShort(HEADER_OFFS_ELENGTH) & 0xFFFF;
				// check that lengths are sensible
				if (length < encodedLength) {
					return; // lengths are not sensible
				}
				// check that the encoding is known: if so, index it
				// an unknown encoding is not an error, but does result in block being skipped
				if (encoding == RAW_ENCODING || encoding == ZLIB_ENCODING) {
					// verify payloads, if set
					// TODO: write code for above
					BlockLocation block = new BlockLocation(hash, startPosition + HEADER_OFFS_PAYLOAD, encoding, (short) length, (short) encodedLength);
					blocksBeforeSync.add(block);
				}
				// advance past the header and payload
				blocksFile.position(startPosition + HEADER_SIZE + encodedLength);
			} else {
				// data here isn't known
				return;
			}
		}
	}
	
	/**
	 * Write a block to the repository and return the hash that can be used to read the data later. The data can be at most 65535 bytes
	 * in size. If writing fails, the repository is closed and RepositoryException is thrown. Note that even if this method successfully
	 * returns, data may not be persisted until a later time.
	 * @param data the data to write
	 * @return the hash of the data
	 * @throws RepositoryException if writing fails
	 * @throws IllegalArgumentException if the data to be written is larger than 65535 bytes
	 * @throws IllegalStateException if the repository is not open or read-only
	 */
	public byte[] writeBlock(ByteBuffer data) {
		checkOpenAndWritable();
		int sourcelength = data.remaining();
		if (sourcelength > 65535) throw new IllegalArgumentException("Data length of " + sourcelength + " is longer than the maximum of 65535 bytes");
		// hash data
		byte[] hash = hash(data);
		lock.lock();
		try {
			if (index.containsKey(hash)) {
				// already in index and file, nothing to write
				return hash;
			}
			// rewind buffer to be read again
			data.rewind();
			// compress data into new buffer to hold encoded payload
			ByteBuffer outData = ByteBuffer.allocate(65535).order(ByteOrder.BIG_ENDIAN); // max size
			// might as well use best compression, since it'll only be compressed once
			boolean zlibSuccess = compress(Deflater.BEST_COMPRESSION, data, outData);
			if (FORCE_RAW) zlibSuccess = false;
			if (!zlibSuccess) {
				// compression failure, just recopy it without compression
				outData.clear();
				data.rewind();
				outData.put(data);
			}
			// outData holds compressed (or not) output, zlibSuccess is true if data is compressed
			// outData: "filling" -> "draining"
			outData.flip();
			// create the final write buffer (holding header and payload) and write header into it
			ByteBuffer writeBuffer = ByteBuffer.allocate(HEADER_SIZE + outData.remaining()).order(ByteOrder.BIG_ENDIAN);
			int encoding = zlibSuccess? ZLIB_ENCODING : RAW_ENCODING;
			int encodedlength = outData.remaining(); // the size of the encoded data
			makeHeader(writeBuffer, hash, encoding, (short) sourcelength, (short) encodedlength);
			// copy the payload into it
			writeBuffer.put(outData);
			// move to end of file
			blocksFile.position(blocksFile.size());
			// before writing, save where the payload is expected to be
			long payloadLocation = blocksFile.position() + HEADER_SIZE;
			// write the block
			writeBuffer.flip();
			writeFully(blocksFile, writeBuffer);
			// update the index
			BlockLocation location = new BlockLocation(hash, payloadLocation, encoding, (short) sourcelength, (short) encodedlength);
			index.put(location.hash(), location);
			return hash;
		} catch (IOException ex) {
			// Problem with writing; shut everything down
			closeFile();
			throw new RepositoryException("Problem while writing block",ex, guessErrorReason(true));
		} finally {
			lock.unlock();
		}
	}
	
	/**
	 * Read a block from the repository for the provided hash, returning null if the data is not present or could not be presented.
	 * If reading fails, RepositoryException or a subclass is thrown; the repository may be closed in some cases.
	 * @param hash The hash for which to request data
	 * @return the data, or null if not available
	 * @throws RepositoryException if there are problems reading the requested data
	 * @throws RecoverableRepositoryException if there are problems, but the repository can still be used
	 */
	public ByteBuffer readBlock(byte[] hash) {
		checkOpen();
		lock.lock();
		try {
			BlockLocation location = index.get(hash);
			if (location == null) return null; // no data present for this hash
			// allocate space for encoded payload and read it
			ByteBuffer encodedPayload = ByteBuffer.allocate(location.blockEncodedLength() & 0xFFFF).order(ByteOrder.BIG_ENDIAN);
			blocksFile.position(location.blockFileLocation());
			readFully(blocksFile, encodedPayload);
			encodedPayload.flip(); // "filling" -> "draining"
			// if data is encoded raw, nothing needs to be done
			if (location.blockEncoding() == RAW_ENCODING) {
				return encodedPayload;
			} else if (location.blockEncoding() == ZLIB_ENCODING) {
				// if zlib encoded, needs a new buffer to hold decompressed data
				ByteBuffer decodedPayload = ByteBuffer.allocate(location.blockLength() & 0xFFFF).order(ByteOrder.BIG_ENDIAN);
				try {
					decompress(encodedPayload, decodedPayload);
				} catch (DataFormatException ex) {
					// data was malformed! treat it as an error
					throw new RecoverableRepositoryException("zlib decompression problem for block " + Hashes.hashToString(hash), ex, Reason.NOT_DECODABLE);
				}
				decodedPayload.flip();
				return decodedPayload;
			} else {
				// format isn't known, so treat it as an error
				throw new RecoverableRepositoryException("Unknown encoding " + Integer.toHexString(location.blockEncoding()) + " for block " + Hashes.hashToString(hash), null, Reason.NOT_DECODABLE);
			}
		} catch (IOException ex) {
			closeFile();
			throw new RepositoryException("Problem while reading block " + Hashes.hashToString(hash),ex, guessErrorReason(false));
		} finally {
			lock.unlock();
		}
	}
	
	/**
	 * Return the SHA3-256 hash of the data.
	 * @param data data to hash
	 * @return SHA3-256 hash of the data
	 */
	public static byte[] hash(ByteBuffer data) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA3-256");
			digest.update(data);
			return digest.digest();
		} catch (NoSuchAlgorithmException ex) {
			throw new RepositoryException("should not happen",ex);
		}
	}
	
	/**
	 * Push header data into the output buffer
	 * @param output output buffer to fill
	 * @param hash hash value
	 * @param encoding block encoding
	 * @param length raw/decompressed length of the payload
	 * @param elength compressed length of the payload
	 */
	private static void makeHeader(ByteBuffer output, byte[] hash, int encoding, short length, short elength) {
		output.order(ByteOrder.BIG_ENDIAN).putLong(HEADER_MAGIC)
			.put(hash)
			.putInt(encoding)
			.putShort(length)
			.putShort(elength);
	}
	
	/**
	 * Fully write this buffer to the channel, repeating until the buffer is drained.
	 * @param chan Channel to write to
	 * @param buf Buffer to drain
	 * @return the number of bytes written, which will be the number of bytes remaining
	 * @throws IOException if an I/O error occurs
	 */
	private static int writeFully(WritableByteChannel chan, ByteBuffer buf) throws IOException {
		int bytes = 0;
		while (buf.hasRemaining()) {
			bytes += chan.write(buf);
		}
		return bytes;
	}
	
	/**
	 * Fully read data into this buffer from the channel, repeating until the buffer is filled or
	 * end of stream is signalled.
	 * @param chan Channel to read from
	 * @param buf Buffer to fill
	 * @return the number of bytes read
	 * @throws IOException if an I/O error occurs
	 */
	private static int readFully(ReadableByteChannel chan, ByteBuffer buf) throws IOException {
		int bytes = 0;
		while (buf.hasRemaining()) {
			int readResult = chan.read(buf);
			if (readResult == -1) {
				return bytes;
			}
			bytes += readResult;
		}
		return bytes;
	}
	
	/**
	 * Compress the input buffer's data into the output buffer, consuming the input buffer's data.
	 * If there is not enough space to hold the output, this method will return <code>false</code>.
	 * In this case, the input buffer will only be partially consumed and the output buffer
	 * filled with the compressed data that did fit. <code>false</code> will also be returned
	 * if ZLIB expands the input.
	 * @param level the ZLIB compression level to use, from 0-9
	 * @param input input data to compress
	 * @param output output buffer for compressed data
	 * @return true on success, false on failure due to lack of space or expansion.
	 */
	private static boolean compress(int level, ByteBuffer input, ByteBuffer output) {
		int inputSize = input.remaining();
		int outputStart = output.position();
		Deflater deflater = new Deflater(level);
		try {
			deflater.setInput(input);
			deflater.finish();
			while (!deflater.finished()) {
				if (!output.hasRemaining()) {
					// out of space! Unlikely unless the input is really incompressible
					return false;
				}
				deflater.deflate(output);
			}
		} finally {
			deflater.end();
		}
		// compression succeeded, but is it actually smaller?
		if (inputSize < output.position()-outputStart) {
			return false; // it isn't, so fail
		}
		return true;
	}
	
	/**
	 * Decompress the input buffer's data into the output buffer, consuming the input buffer's data.
	 * If there is not enough space to hold the output, this method will return <code>false</code>.
	 * In this case, the input buffer will only be partially consumed and the output buffer
	 * filled with the decompressed data that did fit. DataFormatException will be thrown if for some
	 * reason the compressed input is not in valid ZLIB format.
	 * @param input input data to decompress
	 * @param output output buffer for decompressed data
	 * @return true on success, false on failure due to lack of space.
	 * @throws DataFormatException if the input data is malformed
	 */
	private static boolean decompress(ByteBuffer input, ByteBuffer output) throws DataFormatException {
		Inflater inflater = new Inflater();
		try {
			inflater.setInput(input);
			while (!inflater.finished()) {
				if (!output.hasRemaining()) {
					// out of space! likely if the compression ratio is unexpectedly high
					return false;
				}
				inflater.inflate(output);
			}
		} finally {
			inflater.end();
		}
		return true;
	}
	
	/**
	 * Close the block file, wrapping and rethrowing IOException.
	 */
	private void closeFile() {
		try {
			blocksFile.close();
		} catch (IOException ex) {
			throw new RepositoryException("Problem when closing file", ex, guessErrorReason(true));
		}
	}
	
	/**
	 * Throw IllegalStateException if the block file is not open.
	 */
	private void checkOpen() {
		if (!blocksFile.isOpen()) {
			throw new IllegalStateException("Repository is closed!");
		}
	}
	
	/**
	 * Throw IllegalStateException if the block file is not open or not writable.
	 */
	private void checkOpenAndWritable() {
		checkOpen();
		if (readOnly) {
			throw new IllegalStateException("Repository is read-only");
		}
	}
	
	/**
	 * Probe the environment to determine why an I/O operation might have failed
	 * @param writeOperation whether the operation involved writing data
	 * @return a speculated reason
	 */
	private Reason guessErrorReason(boolean writeOperation) {
		// Try to guess why an operation might have failed
		// Try getting the file store; if not able, classify as IO error
		FileStore fs;
		try {
			fs = Files.getFileStore(blocksPath);
			// Have file store, reasons depend on whether a write was attempted
			// do we even have a file?
			if (!Files.exists(blocksPath)) {
				// file not found
				return Reason.FILE_NOT_FOUND;
			}
			if (writeOperation) {
				if (fs.getUsableSpace() < 65535) {
					// Less than 64KB of space on storage device.
					// Write probably failed due to lack of disk space.
					return Reason.NO_SPACE;
				} else {
					// There is 64KB or more space on the device.
					// Write may have hit a filesystem limit
					// it's the best reason we have
					return Reason.BACKEND_LIMIT;
				}
			} else {
				// Not a write operation
				// It's most likely an IO error
				return Reason.IO_ERROR;
			}
		} catch (IOException ex) {
			// file store access failure
			return Reason.IO_ERROR;
		}
	}
}