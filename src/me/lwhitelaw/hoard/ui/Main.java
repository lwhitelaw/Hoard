package me.lwhitelaw.hoard.ui;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.NoSuchAlgorithmException;

import me.lwhitelaw.hoard.ByteTrie;
import me.lwhitelaw.hoard.Repository;

public class Main {
	public static void main(String[] args) throws IOException {
		Path test = Paths.get("E:/ProjectTesting/HoardTest/hoard.blk");
		try {
			Repository repo = new Repository(test,true);
//			byte[] hash = repo.writeBlock(ByteBuffer.wrap("Hello World".getBytes()));
//			byte[] hash2 = repo.writeBlock(ByteBuffer.wrap("Goodbye World".getBytes()));
//			System.out.println(Repository.hashToString(hash));
//			System.out.println(Repository.hashToString(hash2));
			repo.writeBlock(ByteBuffer.wrap("Hello World".getBytes()));
			repo.writeBlock(ByteBuffer.wrap("Hello World".getBytes()));
			byte[] hash = repo.writeBlock(ByteBuffer.wrap("Hello World?".getBytes()));
			repo.writeBlock(ByteBuffer.wrap("Hello World!".getBytes()));
			repo.writeBlock(ByteBuffer.wrap("Hello World".getBytes()));
			System.out.println(StandardCharsets.US_ASCII.decode(repo.readBlock(hash)));
//			repo.sync();
			repo.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
