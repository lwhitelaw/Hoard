package me.lwhitelaw.hoard.ui;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import me.lwhitelaw.hoard.ByteTrie;
import me.lwhitelaw.hoard.FileRepository;
import me.lwhitelaw.hoard.Hashes;
import me.lwhitelaw.hoard.PackfileReader;
import me.lwhitelaw.hoard.PackfileWriter;
import me.lwhitelaw.hoard.RecoverableRepositoryException;
import me.lwhitelaw.hoard.Repository;
import me.lwhitelaw.hoard.RepositoryException;
import me.lwhitelaw.hoard.util.Chunker;
import me.lwhitelaw.hoard.util.SuperblockInputStream;
import me.lwhitelaw.hoard.util.SuperblockOutputStream;

public class Main {
	public static void main(String[] args) throws IOException {
		// check arguments to switch on command
		if (args.length < 1) {
			help();
			return;
		}
		
		// switch on command, check args to run it
		switch (args[0].toLowerCase()) {
			case "write":
				// block write
				// check enough args to call
				if (args.length == 3) {
					write(args[1], args[2]);
				} else {
					help();
				}
				break;
			case "writelong":
				// block write
				// check enough args to call
				if (args.length == 3) {
					writelong(args[1], args[2]);
				} else {
					help();
				}
				break;
			case "read":
				// block read
				// check enough args to call
				if (args.length == 3) {
					read(args[1], args[2]);
				} else {
					help();
				}
				break;
			case "readlong":
				// block read
				// check enough args to call
				if (args.length == 3) {
					readlong(args[1], args[2]);
				} else {
					help();
				}
				break;
			default:
				help();
				break;
		}
	}
	
	private static void help() {
		System.out.println("Hoard CAS file repository manager");
		System.out.println("Usage: <command> <repofile> [args]");
		System.out.println();
		System.out.println("Commands:");
		System.out.println("  write <repofile> <filename>");
		System.out.println("    Write specified file into repository and print hash to standard out");
		System.out.println("    Filesize limited to at most 65535 bytes");
		System.out.println("    Exit code is 0 on success, 255 on error");
		System.out.println();
		System.out.println("  read <repofile> <hash>");
		System.out.println("    Read block referenced by hash from repository and print contents to standard out");
		System.out.println("    Exit code is 0 on success, 1 if data not present, 255 on error");
	}
	
	private static void write(String repofile, String filename) {
		try {
			Path repopath = validatePath(repofile); // where repo will be stored
			Path blockpath = validatePath(filename); // where to source the block data
			validateFile(blockpath,true);
			// Initialise repo
			Repository repo = getRepository(repopath,true);
			
			int exitcode = 0;
			try {
				// Read file and write to repo
				ByteBuffer buffer = ByteBuffer.wrap(Files.readAllBytes(blockpath));
				byte[] hash = repo.writeBlock(buffer);
				System.out.println(Hashes.hashToString(hash));
			} catch (IOException ex) {
				System.err.println("ERROR: I/O error");
				exitcode = 255;
			} catch (RepositoryException ex) {
				System.err.println("ERROR: repository write failed: " + ex.getReason());
				exitcode = 255;
			} finally {
				try {
					repo.close();
				} catch (RepositoryException ex) {
					System.err.println("ERROR: repository failed to close: " + ex.getReason());
					exitcode = 255;
				}
			}
			System.exit(exitcode);
			return;
		} catch (Exception ex) {
			System.err.println("ERROR: unknown exception thrown");
			ex.printStackTrace();
			System.exit(255);
			return; // never happens but keeps javac happy
		}
	}
	
	private static void writelong(String repofile, String filename) {
		try {
			Path repopath = validatePath(repofile); // where repo will be stored
			Path blockpath = validatePath(filename); // where to source the block data
			validateFile(blockpath,false);
			// Initialise repo
			Repository repo = getRepository(repopath,true);
			
			int exitcode = 0;
			try {
				// Open file input stream
				InputStream stream = new BufferedInputStream(Files.newInputStream(blockpath), 65536);
				SuperblockOutputStream sos = new SuperblockOutputStream(repo);
				// Metrics
				long startTime = System.currentTimeMillis();
				long prevSample = startTime;
				long transferred = 0;
				long prevTransferred = 0;
				long total = Files.size(blockpath);
				
				int c;
				while ((c = stream.read()) != -1) {
					sos.write(c);
					transferred++;
					if ((System.currentTimeMillis()-prevSample) >= 2000) {
						long now = System.currentTimeMillis();
						System.out.println(StatusLine.formatTransferProgress(now, prevSample, startTime, transferred, prevTransferred, total, filename));
						prevTransferred = transferred;
						prevSample = now;
//						repo.sync();
					}
				}
				stream.close();
				sos.close();
				byte[] hash = sos.getHash();
				System.out.println(Hashes.hashToString(hash));
			} catch (IOException ex) {
				System.err.println("ERROR: I/O error");
				exitcode = 255;
			} catch (RepositoryException ex) {
				System.err.println("ERROR: repository write failed: " + ex.getReason());
				exitcode = 255;
			} finally {
				try {
					if (repo instanceof PackfileWriter) {
						((PackfileWriter) repo).write(repopath);
					}
					repo.close();
				} catch (RepositoryException ex) {
					System.err.println("ERROR: repository failed to close: " + ex.getReason());
					exitcode = 255;
				}
			}
			System.exit(exitcode);
			return;
		} catch (Exception ex) {
			System.err.println("ERROR: unknown exception thrown");
			ex.printStackTrace();
			System.exit(255);
			return; // never happens but keeps javac happy
		}
	}
	
	private static void read(String repofile, String hash) {
		try {
			Path repopath = validatePath(repofile); // where repo will be stored
			byte[] hasharray = validateHash(hash);
			// Initialise repo
			Repository repo = getRepository(repopath,false);
			
			int exitcode = 0;
			try {
				// Read repo block and write to standard out
				ByteBuffer buf = repo.readBlock(hasharray);
				if (buf == null) {
					// no data here...
					System.out.println("<not present>");
					exitcode = 1;
				} else {
					while (buf.hasRemaining()) {
						System.out.write(buf.get() & 0xFF);
					}
					System.out.flush();
					exitcode = 0;
				}
			} catch (RepositoryException ex) {
				System.err.println("ERROR: repository read failed: " + ex.getReason());
				exitcode = 255;
			} finally {
				try {
					repo.close();
				} catch (RepositoryException ex) {
					System.err.println("ERROR: repository failed to close: " + ex.getReason());
					exitcode = 255;
				}
			}
			System.exit(exitcode);
			return;
		} catch (Exception ex) {
			System.err.println("ERROR: unknown exception thrown");
			ex.printStackTrace();
			System.exit(255);
			return; // never happens but keeps javac happy
		}
	}
	
	private static void readlong(String repofile, String hash) {
		try {
			Path repopath = validatePath(repofile); // where repo will be stored
			byte[] hasharray = validateHash(hash);
			// Initialise repo
			Repository repo = getRepository(repopath,false);
			
			int exitcode = 0;
			try {
				// Read repo block and write to standard out
				SuperblockInputStream sis = new SuperblockInputStream(repo, hasharray);
				int c;
				while ((c = sis.read()) != -1) {
					System.out.write(c & 0xFF);
				}
				System.out.flush();
				sis.close();
				exitcode = 0;
			} catch (RepositoryException ex) {
				System.err.println("ERROR: repository read failed: " + ex.getReason());
				exitcode = 255;
			} finally {
				try {
					repo.close();
				} catch (RepositoryException ex) {
					System.err.println("ERROR: repository failed to close: " + ex.getReason());
					exitcode = 255;
				}
			}
			System.exit(exitcode);
			return;
		} catch (Exception ex) {
			System.err.println("ERROR: unknown exception thrown");
			ex.printStackTrace();
			System.exit(255);
			return; // never happens but keeps javac happy
		}
	}
	
	private static Repository getRepository(Path repopath, boolean writable) {
		checkSHA3();
//		try {
//			return new FileRepository(repopath, writable);
//		} catch (RepositoryException ex) {
//			System.err.println("ERROR: could not open repository at " + repopath + ": " + ex.getReason());
//			System.exit(255);
//			return null; // never happens but keeps javac happy
//		}
		if (writable) {
			return new PackfileWriter(Integer.MAX_VALUE);
		} else {
			try {
				return new PackfileReader(repopath);
			} catch (IOException ex) {
				System.err.println("ERROR: could not open repository at " + repopath);
				System.exit(255);
				return null;
			}
		}
	}
	
	private static void checkSHA3() {
		try {
			MessageDigest.getInstance("SHA3-256");
		} catch (NoSuchAlgorithmException ex) {
			System.err.println("ERROR: SHA3-256 not supported in this Java runtime");
			System.exit(255);
		}
	}
	
	private static Path validatePath(String path) {
		try {
			return Paths.get(path);
		} catch (InvalidPathException ex) {
			System.err.println("ERROR: " + path + " is not a valid path");
			System.exit(255);
			return null;
		}
	}
	
	private static void validateFile(Path path, boolean longCheck) {
		if (!Files.isRegularFile(path)) {
			System.err.println("ERROR: " + path + " is not a file");
			System.exit(255);
			return;
		}
		if (!Files.isReadable(path)) {
			System.err.println("ERROR: " + path + " is not readable");
			System.exit(255);
			return;
		}
		try {
			if (longCheck && Files.size(path) > 65535) {
				System.err.println("ERROR: " + path + " is larger than 65535 bytes");
				System.exit(255);
				return;
			}
		} catch (IOException ex) {
			System.err.println("ERROR: I/O error validating " + path);
			System.exit(255);
			return;
		}
	}
	
	private static byte[] validateHash(String hash) {
		try {
			byte[] hasharray = Hashes.stringToHash(hash);
			if (hasharray.length != 32) { // SHA3-256 bytes
				System.err.println("ERROR: hash is not 256 bits");
				System.exit(255);
				return null;
			}
			return hasharray;
		} catch (IllegalArgumentException ex) {
			System.err.println("ERROR: invalid hash");
			System.exit(255);
			return null;
		}
	}
}
