package me.lwhitelaw.hoard;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class PackfileCollection {
	private final List<PackfileReader> openPackfiles;
	
	public PackfileCollection() {
		openPackfiles = new ArrayList<>();
	}
	
	public void addPackfile(PackfileReader reader) {
		openPackfiles.add(reader);
	}
	
	/**
	 * If the path references a valid packfile, add it. If the path references a folder, recursively browse every file and folder and add every
	 * file that opens as a valid packfile. Otherwise, this method fails silently.
	 * @param path The file path to add
	 */
	public void addPackfile(Path path) {
		if (Files.isDirectory(path)) {
			try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
				for (Path pathInFolder : stream) {
					addPackfile(pathInFolder);
				}
			} catch (IOException | DirectoryIteratorException ex) {
				// swallow deliberately
			}
			return;
		} else if (Files.isRegularFile(path)) {
			PackfileReader newPack;
			try {
				newPack = new PackfileReader(path);
			} catch (IOException ex) {
				return;
			}
			openPackfiles.add(newPack);
		}
	}
	
	/**
	 * Read a block from this collection for the provided hash, returning null if the data is not present or could not be decoded.
	 * Every packfile in the collection will be tried until the data is returned or all fail to return any data.
	 * @param hash The hash to read
	 * @return the data, or null if not present in any packfile
	 */
	public ByteBuffer readBlock(byte[] hash) {
		for (PackfileReader packfile : openPackfiles) {
			ByteBuffer block = packfile.readBlock(hash);
			if (block != null) return block;
		}
		return null;
	}
	
	/**
	 * Close all open packfiles added to this collection.
	 */
	public void close() {
		for (PackfileReader packfile : openPackfiles) {
			packfile.close();
		}
	}
}
