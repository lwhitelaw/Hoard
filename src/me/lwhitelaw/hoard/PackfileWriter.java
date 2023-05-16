package me.lwhitelaw.hoard;

import java.nio.ByteBuffer;

public final class PackfileWriter {
	private ByteBuffer headerArea;
	private ByteBuffer dataArea;
	
	public PackfileWriter(int dataAreaSize) {
		dataArea = ByteBuffer.allocate(dataAreaSize);
	}
}
