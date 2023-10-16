package me.lwhitelaw.hoard;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.DataFormatException;

import me.lwhitelaw.hoard.util.Buffers;
import static me.lwhitelaw.hoard.Format.*;

/**
 * A reader for a Hoard packfile. Blocks may be requested by their hash, or enumerated. Other properties of the Hoard packfile
 * may also be requested. Instances are safe for concurrent use.
 *
 */
public class PackfileReader {
	private final FileChannel file;
	private final long blocktableLength;
	private final long dataAreaStart;
	
	private static final int CACHE_SIZE = (1 << 16);
	private static final int CACHE_MASK = CACHE_SIZE - 1;
	private static record CacheEntry(long index, PackfileEntry value) {}
	private final ThreadLocal<CacheEntry[]> cache;
	
	/**
	 * Open a Hoard packfile at the provided path.
	 * @param filePath The path to the packfile to open
	 * @throws IOException if the file could not be opened or is invalid.
	 */
	public PackfileReader(Path filePath) throws IOException {
		file = FileChannel.open(filePath, StandardOpenOption.READ);
		// Check header and initialise locations
		// Checks are only basic for speed
		{
			ByteBuffer hbuf = ByteBuffer.allocate(HEADER_SIZE).order(ByteOrder.BIG_ENDIAN);
			Buffers.readFileFully(file, hbuf, OFFS_HEADER_START);
			// check EOF
			if (hbuf.hasRemaining()) throw new IOException("Unexpected end of file");
			// check magic is valid
			if (hbuf.getLong(HEADER_OFFS_MAGIC) != HEADER_MAGIC) throw new IOException("Incorrect magic value");
			// extract and check blocktable length
			blocktableLength = hbuf.getLong(HEADER_OFFS_BLOCKTABLE_LENGTH);
			if (blocktableLength < 0) throw new IOException("Blocktable length is invalid: " + blocktableLength);
			// extract and check the data area start that it isn't negative
			dataAreaStart = hbuf.getLong(HEADER_OFFS_DATA_AREA_START);
			if (dataAreaStart < 0) throw new IOException("Data area start is invalid: " + dataAreaStart);
		}
		// Init cache
		cache = new ThreadLocal<CacheEntry[]>();
		cache.set(new CacheEntry[CACHE_SIZE]);
	}
	
	/**
	 * Read a block with the specified hash, returning a buffer holding the data. Null is returned if the block is not present.
	 * The data returned is assumed to be trusted; there is no check that the data returned hashes to the value provided. Clients using
	 * untrusted packfiles should verify the returned data to ensure that this is the case.
	 * @param hash the hash to request
	 * @return the data in a new buffer, or null if not found
	 * @throws IOException if the data could not be read due to an error in reading or decoding
	 */
	public ByteBuffer readBlock(byte[] hash) throws IOException {
		PackfileEntry entry = locateEntryForHash(hash);
		if (entry == null) return null;
		return readPackfileEntryPayload(entry,false);
	}
	
	/**
	 * Enumerate all blocks in this packfile and return their entries. This is a potentially expensive operation.
	 * @return a list of all entries in this packfile
	 * @throws IOException if there is an error while reading the blocktable
	 */
	public List<PackfileEntry> enumerateBlocks() throws IOException {
		ArrayList<PackfileEntry> blockList = new ArrayList<>();
		for (int i = 0; i < blocktableLength; i++) {
			PackfileEntry entry = getBlocktableEntry(i);
			blockList.add(entry);
		}
		return blockList;
	}
	
	/**
	 * Close this packfile. In case of an error, UncheckedIOException is thrown, wrapping the exception that caused it.
	 */
	public void close() {
		try {
			file.close();
		} catch (IOException ex) {
			throw new UncheckedIOException("I/O error on close; this seems rather unlikely", ex);
		}
	}
	
	// Utilities
	
	/**
	 * Get the number of entries in the blocktable.
	 * @return the number of entries
	 */
	public long getBlocktableLength() {
		return blocktableLength;
	}
	
	/**
	 * Get the entry in the block table at the specified index. The index is not checked.
	 * @param index the index into the block table to read
	 * @return the block table entry
	 * @throws IOException if there are problems reading the file or if the entry is malformed
	 */
	public PackfileEntry getBlocktableEntry(long index) throws IOException {
		// Check cache
		{
			CacheEntry cachedCandidate = cache.get()[(int)(index & CACHE_MASK)];
			if (cachedCandidate != null && cachedCandidate.index == index) return cachedCandidate.value;
		}
		// else, read it normally
		ByteBuffer ebuf = ByteBuffer.allocate(ENTRY_SIZE).order(ByteOrder.BIG_ENDIAN);
		long filePosition = (long)ENTRY_SIZE * (long)index + Format.OFFS_BLOCKTABLE_START;
		Buffers.readFileFully(file, ebuf, filePosition);
		// check EOF
		if (ebuf.hasRemaining()) throw new IOException("Unexpected end of file");
		// create object
		// PackfileEntry will do its own checks
		try {
			ebuf.flip();
			PackfileEntry entry = PackfileEntry.fromBuffer(ebuf);
			// valid, write into cache
			cache.get()[(int)(index & CACHE_MASK)] = new CacheEntry(index, entry);
			// return value
			return entry;
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
		long startOfDataArea = dataAreaStart;
		// Calculate file position
		long filePosition = startOfDataArea + entry.getPayloadIndex();
		// Check if payload index makes sense
		// This should not be a problem, but might as well check
		if (filePosition + entry.getEncodedLength() > file.size()) {
			// Overflow detected!
			throw new IOException("Payload index would exceed size of file");
		}
		// Allocate buffer and read encoded data
		ByteBuffer encoded = ByteBuffer.allocate(entry.getEncodedLength());
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
		long low = 0;
		long high = blocktableLength - 1;
		// loop until returned from
		while (low <= high) {
			// Find the midpoint. Calculation is written to avoid overflow.
			long mid = low + (high - low) / 2;
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
