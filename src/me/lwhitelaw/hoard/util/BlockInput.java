package me.lwhitelaw.hoard.util;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface BlockInput {
	ByteBuffer readBlock(byte[] hash) throws IOException;
}