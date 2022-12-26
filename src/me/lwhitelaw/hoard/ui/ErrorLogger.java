package me.lwhitelaw.hoard.ui;

import java.util.ArrayList;
import java.util.List;

public class ErrorLogger {
	private final List<String> strings;
	
	public ErrorLogger() {
		strings = new ArrayList<>();
	}
	
	public void logWarning(String warning) {
		strings.add(warning);
	}
	
	public void printWarnings() {
		for (String s : strings) {
			System.out.println(s);
		}
		if (strings.isEmpty()) {
			System.out.println("OK");
		} else {
			System.out.println(strings.size() + " warnings");
		}
	}
}
