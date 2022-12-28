package me.lwhitelaw.hoard;

/**
 * Thrown when an problem has occurred and the repository is potentially in an
 * indeterminate state.
 */
public class RepositoryException extends RuntimeException {
	public RepositoryException(Exception cause) {
		super(cause);
	}
	
	public RepositoryException(String message, Exception cause) {
		super(message, cause);
	}
}
