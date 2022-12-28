package me.lwhitelaw.hoard;

import java.util.Objects;

/**
 * A sparse trie keyed on byte arrays using a left-child-right-sibling/Knuth transformed binary tree.
 * Null values are treated as empty nodes. Null keys are not permitted.
 * @param <V> type of the value
 */
public class ByteTrie<V> {
	private static class Node<V> {
		private byte keyElement; // not used for empty string; indicates byte value from the key associated with node
		private V value; // null is not a valid value; indicates entry not in use
		private Node<V> child; // child node, going to longer strings where this node is a prefix: "aa" -> "aaa"
		private Node<V> next; // next sibling node, going to strings of same length where last char is different from this: "aa" -> "ab"
	}
	
	private Node<V> root = null; // root of the trie, either null or starts with node for empty string
	
	/**
	 * Construct an empty trie.
	 */
	public ByteTrie() {}
	
	/**
	 * Return the value associated with this key, or null if no value present.
	 * @param key the key to access
	 * @return the associated value, or null
	 */
	public V get(byte[] key) {
		Objects.requireNonNull(key);
		Node<V> currNode = root;
		if (currNode == null) {
			// not even the empty string exists, so obviously nothing else will.
			return null;
		}
		for (int i = 0; i < key.length; i++) {
			// previous node is i-1 (or empty string), so go to it's child to start
			currNode = currNode.child;
			byte byteInKey = key[i];
			// if there is a node at this level, iterate it until we find the
			// right value for this part of the key or run out of nodes
			while (currNode != null && currNode.keyElement != byteInKey) {
				currNode = currNode.next;
			}
			// if currNode is null, then there's no node for this key
			if (currNode == null) { // not found at this level
				return null;
			}
		}
		return currNode.value;
	}
	
	/**
	 * Return true if there is a value associated with this key.
	 * @param key the key to access
	 * @return true if a value is associated
	 */
	public boolean containsKey(byte[] key) {
		Objects.requireNonNull(key);
		Node<V> currNode = root;
		if (currNode == null) {
			// not even the empty string exists, so obviously nothing else will.
			return false;
		}
		for (int i = 0; i < key.length; i++) {
			// previous node is i-1 (or empty string), so go to it's child to start
			currNode = currNode.child;
			byte byteInKey = key[i];
			// if there is a node at this level, iterate it until we find the
			// right value for this part of the key or run out of nodes
			while (currNode != null && currNode.keyElement != byteInKey) {
				currNode = currNode.next;
			}
			// if currNode is null, then there's no node for this key
			if (currNode == null) { // not found at this level
				return false;
			}
		}
		return currNode.value != null;
	}
	
	/**
	 * Insert a value for the provided key.
	 * @param key the key
	 * @param value the value for this key
	 */
	public void put(byte[] key, V value) {
		Objects.requireNonNull(key);
		Objects.requireNonNull(value);
		Node<V> currNode = root;
		if (currNode == null) {
			// Empty string node doesn't exist- create it
			root = new Node<>();
			currNode = root;
		}
		// Iterate the current sibling chain
		for (int i = 0; i < key.length; i++) {
			// previous node is i-1 (or empty string), so go to it's child to start
			if (currNode.child == null) {
				// there's no child, so make one
				currNode.child = new Node<>();
				currNode.child.keyElement = key[i];
			}
			currNode = currNode.child;
			byte byteInKey = key[i];
			// if there is a node at this level, iterate it until we find the
			// right value for this part of the key or run out of nodes
			while (currNode != null && currNode.keyElement != byteInKey) {
				// about to iterate off end and still didn't find the node
				// therefore it does not exist and must be created.
				// then we just so happen to find it on the next iteration
				if (currNode.next == null) {
					currNode.next = new Node<>();
					currNode.next.keyElement = byteInKey;
				}
				currNode = currNode.next;
			}
		}
		// set value
		currNode.value = value;
	}
	
	/**
	 * Remove the mapping for this key.
	 * @param key the key to remove.
	 */
	public void remove(byte[] key) {
		Objects.requireNonNull(key);
		Node<V> currNode = root;
		if (currNode == null) {
			// not even the empty string exists, nothing to remove
			return;
		}
		for (int i = 0; i < key.length; i++) {
			// previous node is i-1 (or empty string), so go to it's child to start
			currNode = currNode.child;
			byte byteInKey = key[i];
			// if there is a node at this level, iterate it until we find the
			// right value for this part of the key or run out of nodes
			while (currNode != null && currNode.keyElement != byteInKey) {
				currNode = currNode.next;
			}
			// if currNode is null, then there's no node for this key
			if (currNode == null) { // not found at this level
				return;
			}
		}
		// found a node, clear value
		currNode.value = null;
		// clean any dead descendents if needed
		cleanDescendents(currNode);
	}
	
	/**
	 * Returns true if the node is "dead", not having a value or any children.
	 * Attempts to remove dead child/next and returns false otherwise.
	 * @param node node to clean descendents from
	 * @return true on dead node, false if node is essential
	 */
	private boolean cleanDescendents(Node<V> node) {
		// If child is dead, remove it, then drop it's reference
		if (node.child != null && cleanDescendents(node.child)) {
			node.child = null;
		}
		// If next is dead, remove it, then drop it's reference
		if (node.next != null && cleanDescendents(node.next)) {
			node.next = null;
		}
		// If this node has no value, and now has no descendents as a result of the above, it's dead too.
		// Tell supercall to drop reference to this node
		if (node.value == null && node.child == null && node.next == null) {
			return true;
		}
		// Otherwise this node is needed
		return false;
	}
	
	/**
	 * Clean any unnecessary nodes from the internal structure of the trie.
	 */
	public void gc() {
		if (root != null && cleanDescendents(root)) {
			root = null;
		}
	}
}
