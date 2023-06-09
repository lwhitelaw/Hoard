package me.lwhitelaw.hoard.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import me.lwhitelaw.hoard.PackfileEntry;
import me.lwhitelaw.hoard.PackfileReader;
import me.lwhitelaw.hoard.PackfileWriter;

/**
 * Operations on packfiles.
 *
 */
public class Packfiles {
	/**
	 * Merge packfiles at srcs... into one packfile at dest. Enough memory is required to hold all packfile entries to be
	 * merged, plus the size of the largest payload present in any of the files. If merging fails, the temporary file created
	 * will be deleted.
	 * @param dest Location of the destination packfile
	 * @param srcs Source packfiles to merge
	 * @return true if the merge succeeded
	 */
	public boolean mergePackfiles(Path dest, Path... srcs) {
		try {
			// Open the writer
			PackfileWriter destWriter = new PackfileWriter(dest);
			// Open each packfile in turn...
			for (Path src : srcs) {
				PackfileReader reader = new PackfileReader(src);
				// Enumerate the blocks in this packfile
				List<PackfileEntry> blocks = reader.enumerateBlocks();
				// For each block...
				for (PackfileEntry block : blocks) {
					// Read the payload and write it to the destination
					ByteBuffer payload = reader.readPackfileEntryPayload(block, false);
					destWriter.writeBlock(payload);
				}
				// Close the current source packfile
				reader.close();
			}
			// Close the destination packfile
			destWriter.close();
			return true;
		} catch (IOException ex) {
			try {
				Files.delete(dest);
			} catch (IOException ex2) {
				// deliberately ignored
			}
			return false;
		}
	}
}
