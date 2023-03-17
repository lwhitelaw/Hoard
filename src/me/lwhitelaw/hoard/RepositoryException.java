package me.lwhitelaw.hoard;

/**
 * Thrown when an problem has occurred and the repository is potentially in an
 * indeterminate state. The repository may not be usable after this exception is thrown.
 * Repository implementations are encouraged to provide a reason programmatically in addition
 * to the standard exception message, if it is possible to do so.
 */
public class RepositoryException extends RuntimeException {
	/**
	 * A reason broadly indicating why the exception was thrown.
	 */
	public static enum Reason {
		/**
		 * The reason is not known or not of any other type in this enum.
		 */
		UNKNOWN,
		/**
		 * The requested block is not present, but was expected to exist.
		 * <br>
		 * <b>Note:</b> Repositories should return <code>null</code> if a requested block is not present
		 * instead of throwing an exception with this reason.
		 * This reason exists so higher-level APIs can return a meaningful error if a block referenced
		 * by another block does not exist, or if API restrictions prevent returning nulls.
		 */
		MISSING_BLOCK,
		/**
		 * The repository's backing file was not found.
		 */
		FILE_NOT_FOUND,
		/**
		 * There is no more storage space on the repository's storage backend.
		 */
		NO_SPACE,
		/**
		 * A limit other than storage space was hit on the repository's storage backend. (i.e.
		 * file size limits on file-based backends)
		 */
		BACKEND_LIMIT,
		/**
		 * The network connection to the remote repository was lost or never established.
		 */
		DISCONNECTED,
		/**
		 * The repository backend is busy and cannot process the request.
		 */
		BUSY,
		/**
		 * The Java virtual machine does not support the hashing algorithm requested by the repository.
		 * Alternatively, the repository does not support the hashing algorithm chosen.
		 */
		ALGORITHM_NOT_SUPPORTED,
		/**
		 * There was an I/O error in the repository's storage backend.
		 */
		IO_ERROR,
		/**
		 * The client does not have sufficient permission to perform the operation.
		 */
		NO_PERMISSION,
		/**
		 * The client is requesting operations too fast and is rate limited.
		 */
		RATE_LIMITED,
		/**
		 * The requested data is available but cannot be decoded.
		 * <br>
		 * <b>Note:</b> Repositories may prefer to return <code>null</code> as if the data
		 * is not present if it is unlikely that the caller will be able to rectify the problem.
		 */
		NOT_DECODABLE
	}
	
	private final Reason reason;
	
	public RepositoryException(Exception cause) {
		super(cause);
		reason = Reason.UNKNOWN;
	}
	
	public RepositoryException(String message, Exception cause) {
		super(message, cause);
		reason = Reason.UNKNOWN;
	}
	
	public RepositoryException(String message, Exception cause, Reason reason) {
		super(message, cause);
		this.reason = reason;
	}
	
	public Reason getReason() {
		return reason;
	}
}
