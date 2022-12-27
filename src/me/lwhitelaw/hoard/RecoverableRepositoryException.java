package me.lwhitelaw.hoard;

/**
 * Thrown when an problem has occurred but the repository is still usable.
 */
public class RecoverableRepositoryException extends RepositoryException {
	public RecoverableRepositoryException(Exception cause) {
		super(cause);
	}

	public RecoverableRepositoryException(String message, Exception cause) {
		super(message, cause);
	}
}
