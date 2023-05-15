package me.lwhitelaw.hoard;

/**
 * Constant values describing important values in a Hoard packfile.
 * All values are big-endian.
 */
public class Format {
	private static final int HEADER_SIZE = 28; // size of the header in bytes
	// Offsets into the packfile header
	private static final int HEADER_OFFS_MAGIC = 0; // Magic value (should be HEADER_MAGIC)
	private static final int HEADER_OFFS_HASH = 8; // Hash type, 8 bytes. Normally "SHA3-256".
	private static final int HEADER_OFFS_HASH_LENGTH = 16; // Byte length of a hash, 1 unsigned byte. Normally 32.
	private static final int HEADER_OFFS_RESERVED1 = 17; // 3 bytes, unused.
	private static final int HEADER_OFFS_BLOCKTABLE_LENGTH = 20; // Signed 32-bit number of entries in the block table
	private static final int HEADER_OFFS_ENTRY_LENGTH = 24; // Signed 32-bit byte length of a single blocktable entry
	private static final int HEADER_OFFS_END = HEADER_SIZE; // End of header.
	// Offsets into a blocktable entry
	private static final int ENTRY_OFFS_HASH = 0; // Hash.
	// Offsets into a blocktable entry after the variable-length hash
	private static final int ENTRY_OFFS_ENCODING = 0; // Compression encoding, 4 bytes
	private static final int ENTRY_OFFS_LENGTH = 4; // Payload size, signed 32-bit
	private static final int ENTRY_OFFS_ELENGTH = 8; // Encoded payload size, signed 32-bit, must be <= payload size
	private static final int ENTRY_OFFS_PAYLOAD = 12; // Pointer to payload from the start of the data area, signed 64-bit
	// Magic values
	private static final long HEADER_MAGIC = 0x486F617264207631L; // "Hoard v1", start of a file
	private static final int RAW_ENCODING = 0x00000000; // raw encoded payload
	private static final int ZLIB_ENCODING = 0x5A4C4942; // "ZLIB", zlib encoded payload
}
