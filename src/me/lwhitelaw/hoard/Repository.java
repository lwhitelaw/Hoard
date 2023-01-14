package me.lwhitelaw.hoard;

import java.io.Closeable;
import java.nio.ByteBuffer;

/**
 * A repository for the storage of data blocks, addressed by the virtually unique, fixed-size hash of their contents.
 * Data blocks may have any length from zero to 65535 bytes. A repository may be writable or read-only; a read-only
 * repository will throw an exception if one attempts to write or sync data. There is no specification on what hash any
 * given implementation accepts, so hashes may not be portable between different repository implementations; however, the
 * use of SHA3-256 as the hashing function is recommended. Implementations should document which hashing implementation they use.
 *
 */
public interface Repository extends Closeable {
	/**
	 * Return the size of the hash in bytes this repository expects to receive. This value is fixed at the time
	 * of repository creation and will not change. Consumers may use this value to size pointers in their data structures.
	 * @return the hash size in bytes
	 */
	int hashSize();
	/**
	 * Write a block to the repository and return a hash that uniquely identifies the written data and can be used to read the data later.
	 * The data can be at most 65535 bytes in size. If writing fails, RepositoryException or a subclass is thrown; the repository may
	 * be closed depending on the implementation. Note that even if this method successfully returns, data may not be persisted until a later time.
	 * @param data the data to write
	 * @return the hash of the data
	 * @throws RepositoryException if writing fails
	 * @throws IllegalArgumentException if the data to be written is larger than 65535 bytes
	 * @throws IllegalStateException if the repository is not open or read-only
	 */
	byte[] writeBlock(ByteBuffer data);

	/**
	 * Read a block from the repository for the provided hash, returning null if the data is not present or could not be decoded.
	 * If reading fails, RepositoryException or a subclass is thrown; the repository may be closed depending on the implementation.
	 * @param hash The hash for which to request data
	 * @return the data, or null if not available
	 * @throws RepositoryException if there are problems reading the requested data
	 * @throws RecoverableRepositoryException if there are problems, but the repository can still be used
	 */
	public ByteBuffer readBlock(byte[] hash);
	
	/**
	 * Close the repository. If the repository is writable,
	 * written data will be committed to non-volatile storage if needed, as if by a call to {@link #sync()}.
	 * @throws RepositoryException if closing fails
	 */
	public void close();
	
	/**
	 * Force any written data to be persisted to backing storage. If the repository is directly backed by
	 * local storage, any data written since the opening of this repository or the last invocation of this method
	 * is guaranteed to be persisted after this method returns. If the repository is not directly backed by local storage,
	 * a best-effort attempt is made to persist data to backing storage, though there is no guarantee as to whether the
	 * invocation will have any effect.
	 * @throws RepositoryException if persistence fails
	 * @throws IllegalStateException if the repository is not open or read-only
	 */
	public void sync();
}
