package me.lwhitelaw.hoard.ui;

/**
 * Utilities useful for status lines.
 *
 */
public class StatusLine {
	private StatusLine() {}
	
	private static final String[] sizePostfixes = {"B","KiB","MiB","GiB","TiB","EiB"};
	
	/**
	 * Format bytes into a string
	 * @param size size in bytes
	 * @return a formatted string
	 */
	public static String formatBytes(long size) {
		float adjustedSize = size;
		int idx = 0;
		while (Math.abs(adjustedSize) >= 1024 && idx < sizePostfixes.length-1) {
			adjustedSize /= 1024;
			idx++;
		}
		return String.format("%.1f",adjustedSize) + " " + sizePostfixes[idx];
	}
	
	/**
	 * Format millisecond durations into a string
	 * @param millis the duration
	 * @return a formatted string
	 */
	public static String formatTime(long millis) {
		// calculate total counts
		long seconds = millis / 1000;
		long minutes = seconds / 60;
		long hours = minutes / 60;
		long days = hours / 24;
		long weeks = days / 7;
		// mod values based on upper unit i.e. 123 seconds -> 3 seconds, since 60 seconds in a minute
		seconds %= 60;
		minutes %= 60;
		hours %= 24;
		days %= 7;
		// weeks are highest unit, no need to mod
		StringBuilder sb = new StringBuilder();
		boolean printedHigherUnit = false; // if true, a unit value above was printed, so add string even if value is zero
		// walk each value, adding it to the string
		// we want to print a string if it is greater than zero
		// or a higher unit was printed earlier
		if (weeks > 0) {
			printedHigherUnit = true;
			sb.append(weeks).append("w");
		}
		if (printedHigherUnit || days > 0) {
			printedHigherUnit = true;
			sb.append(days).append("d");
		}
		if (printedHigherUnit || hours > 0) {
			printedHigherUnit = true;
			sb.append(hours).append("h");
		}
		if (printedHigherUnit || minutes > 0) {
			printedHigherUnit = true;
			sb.append(minutes).append("m");
		}
		// seconds should always print
		sb.append(seconds).append("s");
		return sb.toString();
	}
	
	/**
	 * Format transfer progress into a string
	 * @param nowMillis the current time in milliseconds
	 * @param prevMillis the timestamp of the previous sample
	 * @param startMillis the timestamp the operation started
	 * @param nowTransferred the number of bytes transferred now
	 * @param prevTransferred the number of bytes transferred at the previous sample timestamp
	 * @param amountOfBytes the total number of bytes in the transfer operation
	 * @param extraText an extra string to append
	 * @return a formatted string
	 */
	public static String formatTransferProgress(long nowMillis, long prevMillis, long startMillis, long nowTransferred, long prevTransferred, long amountOfBytes, String extraText) {
		// Determine percent of data transferred
		float percentDone = percentDone(nowTransferred, amountOfBytes);
		// Determine instantaneous rate of data transfer
		float transferRateInstant = bytesPerMillisRate(nowTransferred-prevTransferred, nowMillis-prevMillis);
		// Determine total rate of data transfer as bytes/millisecond
		float transferRateTotal = bytesPerMillisRate(nowTransferred, nowMillis-startMillis);
		long bytesRemaining = amountOfBytes - nowTransferred;
		// Calculate time millis remaining based on time it took to get here
		long remainingTotal = (long) (bytesRemaining / transferRateTotal);
		// Calculate time millis remaining based on the current transfer rate
		long remainingInstant = (long) (bytesRemaining / transferRateInstant);
		// Use the higher of the two values as time remaining
		long timeRemaining = Math.max(remainingTotal, remainingInstant);
		// Format string
		return String.format("%.2f%% %s/s %s/%s %s %s", percentDone, formatBytes((long)(transferRateInstant * 1000)), formatBytes(nowTransferred), formatBytes(amountOfBytes), formatTime(timeRemaining), extraText);
	}
	
	public static float percentDone(long nowTransferred, long totalBytes) {
		return ((float) nowTransferred / (float) totalBytes) * 100.0f;
	}
	
	public static float bytesPerMillisRate(long bytesTransferredDelta, long timeMillisDelta) {
		return ((float) bytesTransferredDelta / (float) Math.max(1, timeMillisDelta));
	}
}
