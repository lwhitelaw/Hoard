package me.lwhitelaw.hoard.util.fs;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.DosFileAttributeView;
import java.nio.file.attribute.DosFileAttributes;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

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
	
	public static Tree fromDirectoryPath(Path path) throws IOException {
		// make sure it's not a symlink
		if (Files.isSymbolicLink(path)) {
			throw new IllegalArgumentException("cannot construct tree from a symlink");
		}
		// make sure the path refers to a directory
		if (!Files.isDirectory(path)) {
			throw new IllegalArgumentException("cannot construct tree from this path");
		}
		// list all files and assemble tree entries
		Tree tree = new Tree();
		Stream<Path> paths = Files.list(path);
		try {
			paths.forEach(p -> {
				// skip if it is not a normal file/dir
				if (!(Files.isRegularFile(p) || Files.isDirectory(p))) return;
				// else add it
				try {
					tree.addEntry(TreeEntry.fromPath(p));
				} catch (IOException ex) {
					// stupid lambda exception handling
					// this unchecked exception will immediately get unwrapped and rethrown... because Java
					throw new UncheckedIOException(ex);
				}
			});
		} catch (UncheckedIOException ex) {
			throw ex.getCause();
		} finally {
			paths.close();
		}
		return tree;
	}
}
