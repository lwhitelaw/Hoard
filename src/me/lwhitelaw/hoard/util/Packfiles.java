package me.lwhitelaw.hoard.util;

import java.nio.file.Path;

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
		return false;
	}
}
