package me.lwhitelaw.hoard.util.fs;

import java.util.ArrayList;

/**
 * A serialisable form of directory tree data.
 */
public final class Tree {
	private final ArrayList<TreeEntry> entries;
	public Tree() {
		entries = new ArrayList<TreeEntry>();
	}

}
