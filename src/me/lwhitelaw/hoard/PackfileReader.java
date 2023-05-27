package me.lwhitelaw.hoard;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.DataFormatException;

import me.lwhitelaw.hoard.util.Buffers;
import static me.lwhitelaw.hoard.Format.*;

public class PackfileReader {
	private final FileChannel file;
	private final long blocktableStart;
	private final int blocktableLength;
	
	public PackfileReader(Path filePath) throws IOException {
		file = FileChannel.open(filePath, StandardOpenOption.READ);
		// Check header and initialise locations
		{
			ByteBuffer hbuf = ByteBuffer.allocate(HEADER_SIZE).order(ByteOrder.BIG_ENDIAN);
			Buffers.readFileFully(file, hbuf, HEADER_OFFS_MAGIC);
			// check EOF
			if (hbuf.hasRemaining()) throw new IOException("Unexpected end of file");
			// check magic is valid
			if (hbuf.getLong(HEADER_OFFS_MAGIC) != HEADER_MAGIC) throw new IOException("Incorrect magic value");
			// extract and check blocktable length
			blocktableLength = hbuf.getInt(HEADER_OFFS_BLOCKTABLE_LENGTH);
			if (blocktableLength < 0) throw new IOException("Blocktable length is invalid: " + blocktableLength);
			// extract and check blocktable start
			blocktableStart = hbuf.getLong(HEADER_OFFS_BLOCKTABLE_START);
			if (blocktableStart < 0) throw new IOException("Blocktable start is invalid: " + blocktableStart);
			// check that start + length * 64 equals the end of file
			if ((blocktableStart + (blocktableLength * ENTRY_SIZE)) != file.size()) {
				throw new IOException("The end of the blocktable does not end the file");
			}
		}
	}
	
	public ByteBuffer readBlock(byte[] hash) {
		try {
			PackfileEntry entry = locateEntryForHash(hash);
			if (entry == null) return null;
			return readPackfileEntryPayload(entry,false);
		} catch (IOException ex) {
			return null;
		}
	}
	
	public void close() {
		try {
			file.close();
		} catch (IOException ex) {
			throw new RuntimeException("I/O error on close; this seems rather unlikely", ex);
		}
	}
	
	// Utilities
	
	public int getBlocktableLength() {
		return blocktableLength;
	}
	
	public long getBlocktableStart() {
		return blocktableStart;
	}
	
	/**
	 * Get the entry in the block table at the specified index.
	 * @param index the index into the block table to read
	 * @return the block table entry
	 * @throws IOException if there are problems reading the file or if the entry is malformed
	 */
	public PackfileEntry getBlocktableEntry(int index) throws IOException {
		ByteBuffer ebuf = ByteBuffer.allocate(ENTRY_SIZE).order(ByteOrder.BIG_ENDIAN);
		long filePosition = (long)ENTRY_SIZE * (long)index + (long)blocktableStart;
//		file.position(filePosition);
		Buffers.readFileFully(file, ebuf, filePosition);
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
	
	/**
	 * Read the payload for a packfile entry, decoding as needed. If doNotDecode is true, then the data will be returned as-is.
	 * @param entry the entry to read
	 * @param doNotDecode if true, do not decode the payload
	 * @return the decoded payload data
	 * @throws IOException if reading is not possible
	 */
	public ByteBuffer readPackfileEntryPayload(PackfileEntry entry, boolean doNotDecode) throws IOException {
		// Calculate position into the file based on the packfile index
		long startOfDataArea = DATA_AREA_OFFS_START;
		// Calculate file position
		long filePosition = startOfDataArea + entry.getPayloadIndex();
		// Check if payload index makes sense
		// This should not be a problem, but might as well check
		if (filePosition + entry.getEncodedLength() > blocktableStart) {
			// Overflow detected!
			throw new IOException("Payload index would overflow data area");
		}
		// Allocate buffer and read encoded data
		ByteBuffer encoded = ByteBuffer.allocate(entry.getEncodedLength());
//		file.position(filePosition);
		Buffers.readFileFully(file, encoded, filePosition);
		if (encoded.hasRemaining()) throw new IOException("Unexpected end of file");
		encoded.flip(); // filling -> draining
		// Check entry encoding type to determine what to do with the data
		if (doNotDecode || entry.getEncoding() == RAW_ENCODING) {
			// Raw encoding, or the user requested that data not be decoded. Return as-is.
			return encoded;
		} else if (entry.getEncoding() == ZLIB_ENCODING) {
			// ZLIB-compressed data encoding. Decompress the encoded data and return that.
			ByteBuffer decompressed = ByteBuffer.allocate(entry.getLength()).order(ByteOrder.BIG_ENDIAN);
			try {
				Compression.decompress(encoded, decompressed);
			} catch (DataFormatException ex) {
				// data was malformed! treat it as an error
				throw new IOException("zlib decompression problem");
			}
			decompressed.flip(); // filling -> draining
			return decompressed;
		} else {
			// the encoding type is not recognised
			throw new IOException("encoding type unknown: " + PackfileEntry.encodingToString(entry.getEncoding()));
		}
	}
	
	/**
	 * Search the block table for the entry named by the provided hash. Null will be returned if it is not present.
	 * @param hash the hash to search for
	 * @return the block table entry, or null if not found
	 * @throws IOException if there is a problem reading the file
	 */
	public PackfileEntry locateEntryForHash(byte[] hash) throws IOException {
		// An empty table will never succeed a search.
		if (blocktableLength == 0) return null;
		// Classical binary search, bounds are inclusive.
		// Failure checks are placed in both comparisons to enforce that neither low nor
		// high leave the bounds of the array over the algorithm's execution.
		int low = 0;
		int high = blocktableLength - 1;
		// loop until returned from
		while (low <= high) {
			// Find the midpoint. Calculation is written to avoid overflow.
			int mid = low + (high - low) / 2;
			// Grab this entry.
			PackfileEntry midEntry = getBlocktableEntry(mid);
			// Run comparison check against the target hash.
			int comparison = Hashes.compare(hash, midEntry.getHash());
			if (comparison > 0) {
				// Hash is greater than the midpoint hash.
				if (mid == high) return null; // no upper entries remain, fail the search
				// Cut to above middle
				low = mid + 1;
			} else if (comparison < 0) {
				// Hash is less than the midpoint hash.
				if (mid == low) return null; // no lower entries remain, fail the search
				// Cut to below middle
				high = mid - 1;
			} else {
				// Hashes are equal! Return it.
				return midEntry;
			}
		}
		throw new AssertionError("low > high, this should not happen");
	}
}
