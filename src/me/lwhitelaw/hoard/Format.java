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
	 * 		int32 blocktableLength;
	 *		// Block table
	 * 		Entry[blocktableLength] blocktable;
	 * 		// Data after this point is not defined, but *is* pointed into by the block table
	 * 		byte[] dataArea
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
	 * 		byte[8] reserved; // reserved for future versions
	 * }
	 */
	// Offsets into the packfile header
	public static final int HEADER_OFFS_MAGIC = 0; // Magic value (should be HEADER_MAGIC)
	public static final int HEADER_OFFS_BLOCKTABLE_LENGTH = 8; // Signed 32-bit number of entries in the block table
	public static final int HEADER_SIZE = 12; // size of the header in bytes (also end)
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
	
}
