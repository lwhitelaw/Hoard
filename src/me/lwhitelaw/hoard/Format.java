package me.lwhitelaw.hoard;

/**
 * Constant values describing important values in a Hoard packfile, along with utility methods to read data present.
 * All values are stored in a packfile big-endian.
 */
public class Format {
	/* 
	 * File format is as follows. See declarations for descriptions
	 * 
	 * File
	 * {
	 * 		// Header
	 * 		byte[8] magic = "Hoard v1";
	 * 		int64 blocktableStart;
	 * 		int32 blocktableLength;
	 * 		byte[44] reserved = { 0x00, 0x00 ... 0x00 } // Pads header to 64 bytes
	 * 		// Data in this area has no defined format, but *is* pointed into by the block table
	 * 		// The only requirement is that a blocktable entry alone provides enough information to extract
	 * 		// and decode block payload data from this area. The data area is padded to 64 byte multiples
	 * 		// so the blocktable starts on such a multiple.
	 * 		byte[] dataArea
	 *		// Block table; blocktableStart points to first entry
	 * 		Entry[blocktableLength] blocktable;
	 * 		<eof>
	 * }
	 * 
	 * Entry
	 * {
	 * 		byte[32] hash;
	 * 		byte[8] encoding;
	 * 		int32 length;
	 * 		int32 encodedLength;
	 * 		int64 payloadPointer; // to byte[encodedLength] in the data area. Pointer is offset from start of this data area.
	 * 		byte[8] reserved; // reserved for future versions. Always zero.
	 * }
	 */
	// Offsets into the packfile header
	public static final int HEADER_OFFS_MAGIC = 0; // Magic value (should be HEADER_MAGIC)
	public static final int HEADER_OFFS_BLOCKTABLE_START = 8; // Signed 64-bit absolute position of the block table start within the file
	public static final int HEADER_OFFS_BLOCKTABLE_LENGTH = 16; // Signed 32-bit number of entries in the block table
	public static final int HEADER_OFFS_RESERVED = 20; // Reserved for future use. 44 zero bytes.
	public static final int HEADER_SIZE = 64; // size of the header in bytes
	public static final int DATA_AREA_OFFS_START = HEADER_SIZE; // Start of the data area
	// Offsets into a blocktable entry
	public static final int ENTRY_OFFS_HASH = 0; // 32-byte hash. (256-bit SHA3)
	public static final int ENTRY_OFFS_ENCODING = 32; // Compression encoding format, 8 bytes
	public static final int ENTRY_OFFS_LENGTH = 40; // Payload size, signed 32-bit
	public static final int ENTRY_OFFS_ELENGTH = 44; // Encoded payload size, signed 32-bit, must be <= payload size
	public static final int ENTRY_OFFS_PAYLOAD = 48; // Pointer to payload from the start of the data area, signed 64-bit
	public static final int ENTRY_OFFS_RESERVED = 56; // 8 bytes of reserved data
	public static final int ENTRY_SIZE = 64;
	// Magic values
	public static final long HEADER_MAGIC = 0x486F6172_64207631L; // "Hoard v1", start of a file
	public static final long RAW_ENCODING = 0x00000000_00000000L; // raw encoded payload
	public static final long ZLIB_ENCODING = 0x00000000_5A4C4942L; // "\x00\x00\x00\x00ZLIB", zlib encoded payload
	
	// Utilities
	/**
	 * Round up to nearest 64 bytes if needed.
	 * @param n number to round up
	 * @return number rounded to multiple of 64
	 */
	public static int roundUp64(int n) {
		final int MASK64 = 0x3F;
		if ((n & MASK64) == 0) return n;
		return (n & MASK64) + 64;
	}
	
	/**
	 * Round up to nearest 64 bytes if needed.
	 * @param n number to round up
	 * @return number rounded to multiple of 64
	 */
	public static long roundUp64(long n) {
		final long MASK64 = 0x3F;
		if ((n & MASK64) == 0) return n;
		return (n & ~MASK64) + 64;
	}
}
