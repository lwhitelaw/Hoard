package me.lwhitelaw.hoard;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import static java.nio.file.StandardOpenOption.*;

/**
 * A repository for content-addressed blocks of data, permitting reads and writes.
 *
 */
public class Repository implements Closeable {
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
	
	private final ReentrantLock lock; // operation lock
	private final ByteTrie<BlockLocation> index; // index organising hashes to offsets in file that contain their blocks
	private final FileChannel blocksFile; // the file where blocks are stored
	private final boolean readOnly; // if true, writing not possible
	private long lastFsyncEndOffset; // file offset where last fsync mark was written; data after is ignored and new writes start here
	
	/**
	 * Access a block repository on the specified file. If <code>writable</code> is <code>true</code>, the repository
	 * will be opened for writing, and the file created if needed. Otherwise, the repository is read-only. If the file
	 * has corrupted data and the repository is opened for writing, the corrupted data will be truncated up to the last
	 * safe point.
	 * @param path file path to the repository file
	 * @param writable true if writing should be allowed
	 * @throws IOException if an error occurs when opening the repository.
	 * @throws RepositoryException if SHA3-256 is not supported by the JDK in use
	 */
	public Repository(Path path, boolean writable) throws IOException {
		try {
			MessageDigest.getInstance("SHA3-256"); // should throw if not present
		} catch (NoSuchAlgorithmException ex) {
			throw new RepositoryException("This JDK does not support SHA3-256 hashing", ex);
		}
		lock = new ReentrantLock();
		index = new ByteTrie<>();
		readOnly = !writable;
		// check writable and open file depending on mode
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
	}

	/**
	 * Close the repository. If the repository is writable,
	 * written data will be committed to non-volatile storage if needed.
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
	 * Commit any written data to non-volatile storage. Upon completion
	 * any blocks previously written are guaranteed to be committed to disk.
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
				throw new RepositoryException("Sync failed", ex);
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
	 * in size. If writing fails, the repository is closed and RepositoryException is thrown.
	 * @param data the data to write
	 * @return the hash of the data
	 * @throws RepositoryException if writing fails
	 * @throws IllegalArgumentException if the data to be written is larger than 65535 bytes
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
			throw new RepositoryException("Problem while writing block",ex);
		} finally {
			lock.unlock();
		}
	}
	
	/**
	 * Read a block from the repository for the provided hash, returning null if the data is not present or could not be presented.
	 * If reading fails, RepositoryException is thrown; the repository may be closed in some cases.
	 * @param hash The hash for which to request data
	 * @return the data, or null if not available
	 * @throws RepositoryException if there are problems reading the requested data
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
					throw new RepositoryException("zlib decompression problem for block " + hashToString(hash), ex);
				}
				decodedPayload.flip();
				return decodedPayload;
			} else {
				// format isn't known, so treat it as an error
				throw new RepositoryException("Unknown encoding " + Integer.toHexString(location.blockEncoding()) + " for block " + hashToString(hash), null);
			}
		} catch (IOException ex) {
			closeFile();
			throw new RepositoryException("Problem while reading block " + hashToString(hash),ex);
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
	
	private static final String HEX_DIGITS = "0123456789ABCDEF";
	
	/**
	 * Convert a hex character to a number.
	 * @param h character to convert
	 * @return value of this character
	 * @throws IllegalArgumentException if the character is not a valid hex digit
	 */
	private static int hexCharToInt(char h) {
		switch (h) {
			case '0': return 0x0;
			case '1': return 0x1;
			case '2': return 0x2;
			case '3': return 0x3;
			case '4': return 0x4;
			case '5': return 0x5;
			case '6': return 0x6;
			case '7': return 0x7;
			case '8': return 0x8;
			case '9': return 0x9;
			case 'a': case 'A': return 0xA;
			case 'b': case 'B': return 0xB;
			case 'c': case 'C': return 0xC;
			case 'd': case 'D': return 0xD;
			case 'e': case 'E': return 0xE;
			case 'f': case 'F': return 0xF;
		}
		throw new IllegalArgumentException("not a digit");
	}
	
	/**
	 * Convert a byte-array hash to a string representation.
	 * @param hash hash to convert
	 * @return the hash as a string
	 */
	public static String hashToString(byte[] hash) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < hash.length; i++) {
			int b = hash[i];
			sb.append(HEX_DIGITS.charAt((b >>> 4) & 0x0F));
			sb.append(HEX_DIGITS.charAt((b >>> 0) & 0x0F));
		}
		return sb.toString();
	}
	
	/**
	 * Convert a string hash to a byte-array representation
	 * @param hash string to convert
	 * @return the hash as a byte array
	 * @throws IllegalArgumentException if the string is not of appropriate length
	 */
	public static byte[] stringToHash(String hash) {
		if (hash.length() % 2 != 0) throw new IllegalArgumentException("hash string not a multiple of 2");
		byte[] out = new byte[hash.length() / 2];
		int o = 0;
		for (int i = 0; i < hash.length(); i += 2) {
			out[o] = (byte) (hexCharToInt(hash.charAt(i)) << 4 | hexCharToInt(hash.charAt(i+1)));
			o++;
		}
		return out;
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
			throw new RepositoryException("Problem when closing file", ex);
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
}

/*
Block format
{
	0 "BLOCKHDR"         // Magic number (8 bytes)
	8 byte hash[32];     // 256-bit hash of the block to follow
	40 byte encoding[4];  // compression mode: '\0\0\0\0' (uncompressed),'ZLIB' (zlib)
	44 ushort length;     // unsigned 16-bit raw length of the data following
	46 ushort elength;    // unsigned 16-bit encoded length of the data following
	48 byte data[elength]; // The payload data
}
*/