package me.lwhitelaw.hoard.util.fs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.DosFileAttributeView;
import java.nio.file.attribute.DosFileAttributes;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import me.lwhitelaw.hoard.Hashes;
import me.lwhitelaw.hoard.util.Buffers;
import me.lwhitelaw.hoard.util.Checks;

public final class TreeEntry {
	public static final int BIT_VALID_MASK =    0x0FFF;
	// Types
	public static final int BIT_VIRTUAL =       0x0400; // if set, the item isn't meant to be a real filesystem object
	public static final int BIT_DIRECTORY =     0x0200; // if set, is directory, otherwise it is a file
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
	
	private final int descBits; // bits describing attributes of this tree entry, type, permissions, etc.
	private final long fileSize; // size of file in bytes, Long.MAX_VALUE if size unknown or file way too long. For directories, size of all files within.
	private final long mtime; // last modified time of file/folder/any file in folder, in seconds
	private final String name; // limited to 2^16-1 bytes in UTF-8 though I doubt anyone really is crazy enough to do this
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
		// check the UTF-8
		{
			int utfLength = name.getBytes(StandardCharsets.UTF_8).length;
			if (utfLength >= 65536) throw new IllegalArgumentException("name UTF-8 length would be over 65535: " + utfLength);
		}
		if (fileSize < 0) throw new IllegalArgumentException("file size cannot be negative");
		if (fileSize > 0 && (bits & BIT_DIRECTORY) == BIT_DIRECTORY) throw new IllegalArgumentException("Directories must have file size of zero");
		if (mtime < 0) throw new IllegalArgumentException("mtime may not be negative");
		this.name = name;
		this.descBits = bits;
		this.fileSize = fileSize;
		this.mtime = mtime;
		this.refHash = hash == null? null : Arrays.copyOf(hash, hash.length);
	}
	
	public TreeEntry withHash(byte[] newHash) {
		return new TreeEntry(name, descBits, fileSize, mtime, newHash);
	}
	
	public TreeEntry withSize(long newSize) {
		return new TreeEntry(name, descBits, newSize, mtime, refHash);
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
		unix: if (!haveAttrs) {
			PosixFileAttributeView aview = Files.getFileAttributeView(path, PosixFileAttributeView.class);
			if (aview == null) break unix; // not supported
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
		}
		// Try getting DOS attributes if the above failed
		dos: if (!haveAttrs) {
			DosFileAttributeView aview = Files.getFileAttributeView(path, DosFileAttributeView.class);
			if (aview == null) break dos; // not supported
			DosFileAttributes attrib = aview.readAttributes();
			// Set the mtime
			mtime = Math.max(attrib.creationTime().to(TimeUnit.SECONDS),attrib.lastModifiedTime().to(TimeUnit.SECONDS));
			// Check if the file is read-only in order to set R/W bits for user.
			if (attrib.isReadOnly()) {
				descBits |= BIT_USER_READ;
			} else {
				descBits |= BIT_USER_READ | BIT_USER_WRITE;
			}
			// Set the user X bit if the file heuristically might be executable
			if (isHeuristicallyWindowsExecutable(name)) descBits |= BIT_USER_EXECUTE;
			haveAttrs = true;
		}
		// If all else fails... interrogate NIO directly
		if (!haveAttrs) {
			BasicFileAttributeView aview = Files.getFileAttributeView(path, BasicFileAttributeView.class);
			BasicFileAttributes attrib = aview.readAttributes();
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
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		// Bits
		sb.append((descBits & BIT_VIRTUAL) != 0?       'V' : '-');
		sb.append((descBits & BIT_DIRECTORY) != 0?     'D' : '-');
		sb.append((descBits & BIT_USER_READ) != 0?     'R' : '-');
		sb.append((descBits & BIT_USER_WRITE) != 0?    'W' : '-');
		sb.append((descBits & BIT_USER_EXECUTE) != 0?  'X' : '-');
		sb.append((descBits & BIT_GROUP_READ) != 0?    'R' : '-');
		sb.append((descBits & BIT_GROUP_WRITE) != 0?   'W' : '-');
		sb.append((descBits & BIT_GROUP_EXECUTE) != 0? 'X' : '-');
		sb.append((descBits & BIT_OTHER_READ) != 0?    'R' : '-');
		sb.append((descBits & BIT_OTHER_WRITE) != 0?   'W' : '-');
		sb.append((descBits & BIT_OTHER_EXECUTE) != 0? 'X' : '-');
		// Everything else
		sb.append(String.format(" %20d %s   %s", fileSize, dateTimestampToString(mtime), name));
		return sb.toString();
	}
	
	public static String dateTimestampToString(long epoch) {
		Instant instant = Instant.ofEpochSecond(epoch);
		ZonedDateTime zdt = instant.atZone(ZoneOffset.UTC);
		return String.format("%04d-%02d-%02d %02d:%02d:%02d",
				zdt.getYear(),
				zdt.getMonthValue(),
				zdt.getDayOfMonth(),
				zdt.getHour(),
				zdt.getMinute(),
				zdt.getSecond()
		);
	}
	
	private static final String[] WINDOWS_EXECUTABLE_EXTENSIONS = {"exe","com","bat","msi"};
	
	public static boolean isHeuristicallyWindowsExecutable(String pathend) {
		String lowercasedPath = pathend.toLowerCase(Locale.ROOT);
		for (String ext : WINDOWS_EXECUTABLE_EXTENSIONS) {
			if (lowercasedPath.endsWith(ext)) return true;
		}
		return false;
	}
	
	public void write(ByteBuffer buf) {
		buf.order(ByteOrder.BIG_ENDIAN);
		buf.putShort((short) descBits);
		buf.putLong(fileSize);
		buf.putLong(mtime);
		buf.put(refHash);
		Buffers.putShortString(buf, name);
	}
	
	public static TreeEntry read(ByteBuffer buf) throws IOException {
		buf.order(ByteOrder.BIG_ENDIAN);
		int descBits = buf.getShort() & BIT_VALID_MASK;
		long fileSize = Checks.checkPositive(buf.getLong(),"file size");
		long mtime = Checks.checkPositive(buf.getLong(),"mtime");
		byte[] refHash = new byte[Hashes.HASH_SIZE];
		buf.get(refHash);
		String name = Buffers.getShortString(buf);
		return new TreeEntry(name, descBits, fileSize, mtime, refHash);
	}
}
