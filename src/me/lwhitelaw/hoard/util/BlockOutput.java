package me.lwhitelaw.hoard.util;

import java.nio.ByteBuffer;

public interface BlockOutput {
	byte[] writeBlock(ByteBuffer input);
}