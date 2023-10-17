package me.lwhitelaw.hoard.util.fs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A serialisable form of directory tree data.
 */
public final class Tree {
	private final ArrayList<TreeEntry> entries;
	public Tree() {
		entries = new ArrayList<TreeEntry>();
	}

	public void addEntry(TreeEntry entry) {
		// Sorted insertion
		for (int i = 0; i < entries.size(); i++) {
			int comparison = entry.getName().compareTo(entries.get(i).getName());
			if (comparison == 0) {
				// Equal strings, so replace the existing value
				entries.set(i, entry);
				return;
			} else if (comparison < 0) {
				// Entry here is lexicographically later.
				// Insert here.
				entries.add(i, entry);
				return;
			}
		}
		// Add to end if everything else is smaller
		entries.add(entry);
	}
	
	public void removeEntry(String name) {
		for (int i = 0; i < entries.size(); i++) {
			if (entries.get(i).getName().equals(name)) {
				entries.remove(i);
				return;
			}
		}
	}
	
	public List<TreeEntry> getListView() {
		return Collections.unmodifiableList(entries);
	}
}
