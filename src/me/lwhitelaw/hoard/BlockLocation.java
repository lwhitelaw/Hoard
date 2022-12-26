package me.lwhitelaw.hoard;

/**
 * The data needed to locate and extract a block payload.
 *
 */
record BlockLocation(byte[] hash, long blockFileLocation, int blockEncoding, short blockLength, short blockEncodedLength) {}