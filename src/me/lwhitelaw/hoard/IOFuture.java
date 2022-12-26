package me.lwhitelaw.hoard;

public final class IOFuture<T> {
	private T value;
	private boolean resolved;
	
	public IOFuture() {
		value = null;
		resolved = false;
	}
	
	public void resolve(T value) {
		synchronized (this) {
			if (resolved) throw new IllegalStateException("Already resolved");
			resolved = true;
			this.value = value;
			notifyAll();
		}
	}
	
	public boolean isResolved() {
		synchronized (this) {
			return resolved;
		}
	}
	
	public T get() throws InterruptedException {
		synchronized (this) {
			while (!resolved) {
				wait();
			}
			return value;
		}
	}
}
