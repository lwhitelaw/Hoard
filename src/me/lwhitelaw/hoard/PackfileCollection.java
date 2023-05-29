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
	
	/**
	 * Add the provided packfile reader.
	 * @param reader Opened packfile to add
	 */
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
	 * @throws IOException if an I/O error occurs
	 */
	public ByteBuffer readBlock(byte[] hash) throws IOException {
		for (PackfileReader packfile : openPackfiles) {
			ByteBuffer block = packfile.readBlock(hash);
			if (block != null) return block;
		}
		return null;
	}
	
	/**
	 * Return true if any of the packfiles contains the block referenced by the provided hash.
	 * @param hash The hash to check
	 * @return true if the block exists
	 * @throws IOException if an I/O error occurs
	 */
	public boolean checkExists(byte[] hash) throws IOException {
		for (PackfileReader packfile : openPackfiles) {
			PackfileEntry entry = packfile.locateEntryForHash(hash);
			if (entry != null) return true;
		}
		return false;
	}
	
	/**
	 * Close all open packfiles added to this collection.
	 */
	public void close() {
		for (PackfileReader packfile : openPackfiles) {
			packfile.close();
		}
	}
	
	/**
	 * Generate a path of the form "pack????.hdb", where ???? is the first unused integer starting from 0000. Further digits will be added as needed.
	 * @param folder the folder to generate a path in
	 * @return a path unused by any file
	 */
	public static Path generatePackfilePath(Path folder) {
		if (!Files.isDirectory(folder)) throw new IllegalArgumentException("provided path is not a folder");
		int pack = 0;
		Path path = folder.resolve(String.format("pack%04d.hdb",pack));
		while (!Files.notExists(path)) {
			pack++;
			path = folder.resolve(String.format("pack%04d.hdb",pack));
		}
		return path;
	}
}
