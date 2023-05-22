package me.lwhitelaw.hoard;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * An entry in a packfile's block table. This class is immutable. While the hash array may be mutated,
 * it should also be treated as if it were immutable.
 *
 */
public class PackfileEntry {
	public static final int BYTES = Format.ENTRY_SIZE;
	
	private final byte[] hash;
	private final long encoding;
	private final int length;
	private final int encodedLength;
	private final long payloadIndex;
	
	public PackfileEntry(byte[] hash, long encoding, int length, int encodedLength, long payloadIndex) {
		this.hash = hash;
		this.encoding = encoding;
		this.length = length;
		this.encodedLength = encodedLength;
		this.payloadIndex = payloadIndex;
		// check length >= encodedLength
		if (!(length >= encodedLength)) throw new IllegalArgumentException("Length is smaller than encoded length");
		// check payload pointer not negative
		if (payloadIndex < 0) throw new IllegalArgumentException("Payload index is negative");
	}
	
	public byte[] getHash() {
		return hash;
	}
	
	public long getEncoding() {
		return encoding;
	}
	
	public int getLength() {
		return length;
	}
	
	public int getEncodedLength() {
		return encodedLength;
	}
	
	public long getPayloadIndex() {
		return payloadIndex;
	}
	
	public static PackfileEntry fromBuffer(ByteBuffer buf) {
		buf.order(ByteOrder.BIG_ENDIAN);
		byte[] hash = new byte[32];
		buf.get(hash);
		long encoding = buf.getLong();
		int length = buf.getInt();
		int encodedLength = buf.getInt();
		long payloadIndex = buf.getLong();
		@SuppressWarnings("unused")
		long reserved = buf.getLong(); // not used, but advances the buffer position
		return new PackfileEntry(hash, encoding, length, encodedLength, payloadIndex);
	}
	
	public void toBuffer(ByteBuffer buf) {
		buf.order(ByteOrder.BIG_ENDIAN);
		buf.put(hash);
		buf.putLong(encoding);
		buf.putInt(length);
		buf.putInt(encodedLength);
		buf.putLong(payloadIndex);
		buf.putLong(0x00000000_00000000L); // reserved
	}
	
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof PackfileEntry)) return false;
		PackfileEntry other = (PackfileEntry) obj;
		return Arrays.equals(hash, other.hash)
				&& encoding == other.encoding
				&& length == other.length
				&& encodedLength == other.encodedLength
				&& payloadIndex == other.payloadIndex;
	}
	
	@Override
	public int hashCode() {
		int h = Arrays.hashCode(hash);
		h = 31 * h + (int)(encoding ^ (encoding >>> 32));
		h = 31 * h + length;
		h = 31 * h + encodedLength;
		h = 31 * h + (int)(payloadIndex ^ (payloadIndex >>> 32));
		return h;
	}
	
	@Override
	public String toString() {
		return String.format("{%s,'%s',%d (encoded: %d), index: %016X}", Hashes.hashToString(hash),encodingToString(encoding),length,encodedLength,payloadIndex);
	}
	
	public static String encodingToString(long encoding) {
		StringBuilder sb = new StringBuilder();
		for (int i = 7; i >= 0; i--) {
			int targetByte = ((int)(encoding >>> (8*i))) & 0xFF;
			if (targetByte >= 32 && targetByte <= 126) {
				sb.append((char) targetByte);
			} else if (targetByte < 10) {
				sb.append("\\x0").append(Integer.toHexString(targetByte));
			} else {
				sb.append("\\x").append(Integer.toHexString(targetByte));
			}
		}
		return sb.toString();
	}
}
