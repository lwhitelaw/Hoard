package me.lwhitelaw.hoard.util;

import java.util.ArrayList;

/**
 * A parser for command-line arguments vaguely similar to legacy getopt.
 *
 */
public class OptionParser {
	/**
	 * Raw args from main
	 */
	private final String[] argv;
	/**
	 * The parse string, same format as getopt
	 */
	private final String getoptDecl;
	/**
	 * The index into argv to read from
	 */
	private int idx = 0;
	/**
	 * Remaining parts of the string that need parsing
	 */
	private String remaining;
	/**
	 * The argument just parsed
	 */
	private String arg;
	/**
	 * The argument that was not matched
	 */
	private char badopt;
	/**
	 * If true, argument parsing is suppressed after this point
	 */
	private boolean argParsingDisabled = false;
	/**
	 * Filtered arguments.
	 */
	private final ArrayList<String> filteredArgv;
	
	public OptionParser(String[] argv, String getoptDecl) {
		this.argv = argv;
		this.getoptDecl = getoptDecl;
		this.filteredArgv = new ArrayList<String>();
		remaining = null;
		arg = null;
		badopt = '\0';
	}
	
	public int getopt() {
		if (remaining == null) {
			// No left-over remaining chars to handle.
			// Find an arg to process.
			String nextOpt = seekNextArgString();
			if (nextOpt == null) {
				// no more
				return -1;
			}
			// save these opt chars and point to next argv for later opts/args
			idx++;
			remaining = nextOpt;
		}
		// Get the opt char
		char optchar = remaining.charAt(0);
		// Clear the badopt and optarg
		badopt = '\0';
		arg = null;
		// Consume the arg character
		if (remaining.length() > 1) {
			remaining = remaining.substring(1);
		} else {
			remaining = null; // null if no chars would remain
		}
		// Check that opt is --, if so, opt parsing is suppressed and return the -
		if (optchar == '-') {
			argParsingDisabled = true;
			return (int) '-';
		}
		// Determine if the arg should exist or requires args
		int idxInArgList = indexInArgList(optchar);
		if (idxInArgList == -1) {
			// not in list!
			badopt = optchar;
			return (int) '?';
		}
		// Does it require an argument?
		if (requiresArg(idxInArgList)) {
			if (remaining != null) {
				// get the arg from the remainder of the string and consume them
				// throw remaining chars into optarg
				arg = remaining;
				remaining = null;
			} else {
				// the next argv becomes the arg, if it exists
				if (idx >= argv.length) {
					// no argv items remain, report error
					badopt = optchar;
					return (int) ':';
				}
				// get next argv and save that as the optarg
				arg = argv[idx]; // this is idx + 1 from the earlier increment
				idx++; // skip past the arg item
			}
		}
		// finally return the opt we matched
		return optchar;
	}
	
	public String optarg() {
		return arg;
	}
	
	public char optopt() {
		return badopt;
	}
	
	public String[] getArgv() {
		return filteredArgv.toArray(new String[] {});
	}
	
	/**
	 * Iterate argv until encountering a string starting with "-" and longer than 1 char. If so, return it without the "-".
	 * Otherwise, add encountered strings to filteredArgv. Return null if no more strings remain.
	 * @return an argstring or null if no more remain
	 */
	private String seekNextArgString() {
		while (idx < argv.length) {
			if (!argParsingDisabled && argv[idx].startsWith("-") && argv[idx].length() > 1) {
				return argv[idx].substring(1);
			} else {
				filteredArgv.add(argv[idx]);
				idx++;
			}
		}
		return null;
	}
	
	private int indexInArgList(char c) {
		return getoptDecl.indexOf(c);
	}
	
	private boolean requiresArg(int index) {
		if (getoptDecl.length() == 0) return false;
		if (index == getoptDecl.length() - 1) return false;
		return getoptDecl.charAt(index + 1) == ':';
	}
}
