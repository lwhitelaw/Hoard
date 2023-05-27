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
	 */
	public ByteBuffer readBlock(byte[] hash) {
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
	
	// TODO: remove these
	
	public byte[] writeBlockIntoPackfileSeries(PackfileWriter writer, Path folder, ByteBuffer buffer) throws IOException {
		// Check if the block already exists.
		// The precalculated hash can be saved in case the block needs to be written.
		byte[] hash = Hashes.doHash(buffer.duplicate());
		if (checkExists(hash)) return hash;
		// Check if there is sufficient room to write.
		if (writer.remainingCapacity() < buffer.remaining()) {
			// Not enough room. Write to folder and reset writer.
			writer.write(generatePackfilePath(folder));
			writer.reset();
		}
		// Check again to see if there's enough room.
		if (writer.remainingCapacity() < buffer.remaining()) {
			// Not enough room. Create new packfile writer just for this block.
			PackfileWriter largeObjectWriter = new PackfileWriter(buffer.remaining());
			largeObjectWriter.writeBlockUnsafe(buffer,hash);
			largeObjectWriter.write(generatePackfilePath(folder));
			return hash;
		} else {
			// There is enough room, so write the block.
			return writer.writeBlockUnsafe(buffer,hash);
		}
	}
	
	public void finishPackfileSeries(PackfileWriter writer, Path folder) throws IOException {
		if (writer.isEmpty()) return;
		writer.write(generatePackfilePath(folder));
		writer.reset();
	}
}
