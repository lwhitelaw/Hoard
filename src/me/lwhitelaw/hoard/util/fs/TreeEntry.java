package me.lwhitelaw.hoard.util.fs;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.DosFileAttributes;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public final class TreeEntry {
	public static final int BIT_VALID_MASK =    0x03FF;
	// Types
	public static final int BIT_DIRECTORY =     0x0200; // otherwise it is a file
	// Unix-style permissions, emulated on other platforms
	public static final int BIT_USER_READ =     0x0100;
	public static final int BIT_USER_WRITE =    0x0080;
	public static final int BIT_USER_EXECUTE =  0x0040;
	public static final int BIT_GROUP_READ =    0x0020;
	public static final int BIT_GROUP_WRITE =   0x0010;
	public static final int BIT_GROUP_EXECUTE = 0x0008;
	public static final int BIT_OTHER_READ =    0x0004;
	public static final int BIT_OTHER_WRITE =   0x0002;
	public static final int BIT_OTHER_EXECUTE = 0x0001;
	
	private final String name; // limited to 2^16-1 bytes in UTF-8 though I doubt anyone really is crazy enough to do this
	private final int descBits; // bits describing attributes of this tree entry, type, permissions, etc.
	private final long fileSize; // zero for directories, Long.MAX_VALUE if size unknown or file way too long
	private final long mtime; // last modified time of file/folder/any file in folder, in seconds
	/*
	 * RE: mtime
	 * Hoard's concept of mtime is different from the filesystem.
	 * For a file, this is the mtime or ctime, whichever is newest.
	 * For a directory, this is the mtime or ctime of the directory itself,
	 * or the newest mtime/ctime of *any* file/directory located within, recursively.
	 * 
	 * The intent is that if any file on the real filesystem is newer than this time, a backup
	 * should trigger for that file/folder.
	 */
	private final byte[] refHash; // reference to a block stream this entry refers to. Null if not yet known. If null, the entry must not be written out.
	
	public TreeEntry(String name, int bits, long fileSize, long mtime, byte[] hash) {
		if (name == null) throw new NullPointerException("name is null");
		if (!(name.length() > 0 && name.length() < 65536)) throw new IllegalArgumentException("name size is less than zero or over 65535: " + name.length());
		if (fileSize < 0) throw new IllegalArgumentException("file size cannot be negative");
		if (fileSize > 0 && (bits & BIT_DIRECTORY) == BIT_DIRECTORY) throw new IllegalArgumentException("Directories must have file size of zero");
		if (mtime < 0) throw new IllegalArgumentException("mtime may not be negative");
		this.name = name;
		this.descBits = bits;
		this.fileSize = fileSize;
		this.mtime = mtime;
		this.refHash = Arrays.copyOf(hash, hash.length);
	}
	
	public TreeEntry withHash(byte[] newHash) {
		return new TreeEntry(name, descBits, fileSize, mtime, newHash);
	}
	
	public static TreeEntry fromPath(Path path) throws IOException {
		// make sure it's not a symlink
		if (Files.isSymbolicLink(path)) {
			throw new IllegalArgumentException("cannot construct tree entry from a symlink");
		}
		// make sure the path refers to either a normal file or a directory
		if (!(Files.isRegularFile(path) || Files.isDirectory(path))) {
			throw new IllegalArgumentException("cannot construct tree entry from this path");
		}
		// get the name
		String name = path.getFileName().toString();
		// check that it'll convert to UTF-8 under the limit
		{
			byte[] nameToUTF8 = name.getBytes(StandardCharsets.UTF_8);
			if (nameToUTF8.length >= 65536) throw new IOException("name too long");
		}
		// fill out the bits, sizes, and mtimes
		int descBits = 0x0000;
		long fileSize = 0; // default to zero
		long mtime = 0; // default to 1970
		// set directory bit if needed. If it isn't a directory, get the file size
		if (Files.isDirectory(path)) {
			descBits |= BIT_DIRECTORY;
			fileSize = 0;
		} else {
			fileSize = Files.size(path);
		}
		
		boolean haveAttrs = false;
		// try getting Unix/POSIX attributes if we do not have attributes already
		if (!haveAttrs) {
			try {
				PosixFileAttributeView aview = Files.getFileAttributeView(path, PosixFileAttributeView.class);
				PosixFileAttributes attrib = aview.readAttributes();
				// Set the mtime
				mtime = Math.max(attrib.creationTime().to(TimeUnit.SECONDS),attrib.lastModifiedTime().to(TimeUnit.SECONDS));
				// Get the permission bits and set those
				Set<PosixFilePermission> permbits = attrib.permissions();
				if (permbits.contains(PosixFilePermission.OWNER_READ)) descBits |= BIT_USER_READ;
				if (permbits.contains(PosixFilePermission.OWNER_WRITE)) descBits |= BIT_USER_WRITE;
				if (permbits.contains(PosixFilePermission.OWNER_EXECUTE)) descBits |= BIT_USER_EXECUTE;
				if (permbits.contains(PosixFilePermission.GROUP_READ)) descBits |= BIT_GROUP_READ;
				if (permbits.contains(PosixFilePermission.GROUP_WRITE)) descBits |= BIT_GROUP_WRITE;
				if (permbits.contains(PosixFilePermission.GROUP_EXECUTE)) descBits |= BIT_GROUP_EXECUTE;
				if (permbits.contains(PosixFilePermission.OTHERS_READ)) descBits |= BIT_OTHER_READ;
				if (permbits.contains(PosixFilePermission.OTHERS_WRITE)) descBits |= BIT_OTHER_WRITE;
				if (permbits.contains(PosixFilePermission.OTHERS_EXECUTE)) descBits |= BIT_OTHER_EXECUTE;
				haveAttrs = true;
			} catch (UnsupportedOperationException ex) {
				// try something else
			}
		}
		// Try getting DOS attributes if the above failed
		if (!haveAttrs) {
			try {
				DosFileAttributes attrib = Files.readAttributes(path, DosFileAttributes.class);
				// Set the mtime
				mtime = Math.max(attrib.creationTime().to(TimeUnit.SECONDS),attrib.lastModifiedTime().to(TimeUnit.SECONDS));
				// Check if the file is read-only in order to set R/W bits for user.
				if (attrib.isReadOnly()) {
					descBits |= BIT_USER_READ;
				} else {
					descBits |= BIT_USER_READ | BIT_USER_WRITE;
				}
				// Set the user X bit if the file heuristically might be executable
				// TODO: write that
				haveAttrs = true;
			} catch (UnsupportedOperationException ex) {
				// try something else
			}
		}
		// If all else fails... interrogate NIO directly
		if (!haveAttrs) {
			BasicFileAttributes attrib = Files.readAttributes(path, BasicFileAttributes.class);
			// Set the mtime
			mtime = Math.max(attrib.creationTime().to(TimeUnit.SECONDS),attrib.lastModifiedTime().to(TimeUnit.SECONDS));
			if (Files.isReadable(path)) descBits |= BIT_USER_READ;
			if (Files.isWritable(path)) descBits |= BIT_USER_WRITE;
			if (Files.isExecutable(path)) descBits |= BIT_USER_EXECUTE;
			haveAttrs = true;
		}
		// assemble and return tree entry
		return new TreeEntry(name, descBits, fileSize, mtime, null);
	}
	
	public String getName() {
		return name;
	}
	
	public int getDescBits() {
		return descBits;
	}
	
	public long getFileSize() {
		return fileSize;
	}
	
	public long getModifiedTime() {
		return mtime;
	}
	
	public byte[] getHash() {
		return Arrays.copyOf(refHash,refHash.length);
	}
}
