package me.lwhitelaw.hoard.util;

import java.io.IOException;

public class Checks {
	public static long checkExpectedValue(long value, long expected, String identifier) throws IOException {
		if (value != expected) throw new IOException("value is not expected: " + identifier);
		return value;
	}
	
	public static int checkExpectedValue(int value, int expected, String identifier) throws IOException {
		if (value != expected) throw new IOException("value is not expected: " + identifier);
		return value;
	}
	
	public static long checkPositive(long value, String identifier) throws IOException {
		if (value < 0) throw new IOException("value not positive: " + identifier);
		return value;
	}
	
	public static int checkPositive(int value, String identifier) throws IOException {
		if (value < 0) throw new IOException("value not positive: " + identifier);
		return value;
	}
	
	public static int checkSufficientSize(int value, int expected, String identifier) throws IOException {
		if (value < expected) throw new IOException("insufficient size: " + identifier);
		return value;
	}
}
