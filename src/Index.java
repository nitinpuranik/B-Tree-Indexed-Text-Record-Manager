package database;

import database.helperClasses.Node;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class Index {

	// An object of the Node class which will be the root for this given index
	private Node root;

	// A variable to hold the index name
	private String indexName;

	// A variable which tells us which column name has been indexed
	private String column;

	// A variable to tell us what file this index belongs to
	private String fileName;

	// A collection of nodes
	private ArrayList<Node> nodeCollection;

	// The maximum number of keys
	private int maxNumOfKeys = 8;

	// --------------------------------------------------------------------------------------
	// The constructor for the Index class
	// This constructor is called when creating a new index
	public Index(String indexName, String column, String fileName) {
		root = null;
		this.indexName = indexName;
		this.column = column;
		this.fileName = fileName;
		nodeCollection = new ArrayList<Node>();
	}

	// --------------------------------------------------------------------------------------
	// Another overloaded constructor for the index class
	// This constructor is called when restoring the index
	public Index(String indexName, String column, String fileName,
			Node root, ArrayList<Node> nodeCollection) {
		this.indexName = indexName;
		this.column = column;
		this.fileName = fileName;
		this.root = root;
		this.nodeCollection = nodeCollection;
	}

	// --------------------------------------------------------------------------------------
	// Getter for indexName

	public String getIndexName() {
		return indexName;
	}

	// --------------------------------------------------------------------------------------
	// Getter for column

	public String getColumn() {
		return column;
	}

	// --------------------------------------------------------------------------------------
	// Getter for fileName
	
	public String getFileName() {
		return fileName;
	}

	// --------------------------------------------------------------------------------------
	// This method inserts a new key value pair to the B+ tree index

	public void insertIntoIndex(String key, int value) {
		if(root == null) {
			root = new Node();
			nodeCollection.add(root);
			insertIntoBTree(root, key, value);
		}
		else {
			//Find the leaf node into which the key value needs to be inserted
			Node leafNode = findInsertionNode(key);
			insertIntoBTree(leafNode, key, value);
		}
	}

	// --------------------------------------------------------------------------------------
	// This method finds the leaf node where a new key value pair needs
	// to be inserted

	private Node findInsertionNode(String key) {
		int loopCounter;
		int tempCounter;

		if(root == null) {
			return null;
		}

		Node insertionNode = root;

		while(insertionNode.isLeafNode == false) {
			tempCounter = insertionNode.numOfKeysPresent;

			for(loopCounter = 0; loopCounter < tempCounter; loopCounter++) {
				if(insertionNode.key.get(loopCounter).compareTo(key) > 0) {
					insertionNode = insertionNode.pointer.get(loopCounter);
					break;
				}
			}

			if(loopCounter == tempCounter) {
				insertionNode = insertionNode.pointer.get(loopCounter);
			}
		}
		return insertionNode;
	}

	// --------------------------------------------------------------------------------------
	// This method inserts new key value pairs into the B+ tree at the leaf level and calls
	// recursive insert to ensure that the tree is balanced

	private void insertIntoBTree(Node node, String key, int value) {
		if(node.numOfKeysPresent < maxNumOfKeys) {
			node.key.add(key);
			Collections.sort(node.key);
			node.numOfKeysPresent++;
			int keyPosition = node.key.indexOf(key);
			node.recordId.add(keyPosition, value);
		}
		else {
			node.key.add(key);
			Collections.sort(node.key);
			node.numOfKeysPresent++;
			int keyPosition = node.key.indexOf(key);
			node.recordId.add(keyPosition, value);

			Node newNode = new Node();
			nodeCollection.add(newNode);
			newNode.nextNodePointer = node.nextNodePointer;
			newNode.prevNodePointer = node;
			node.nextNodePointer = newNode;
			if(newNode.nextNodePointer != null) {
				newNode.nextNodePointer.prevNodePointer = newNode;
			}

			for(int i = maxNumOfKeys/2; i < node.numOfKeysPresent; i++) {
				newNode.key.add(node.key.get(i));
				newNode.recordId.add(node.recordId.get(i));
				newNode.numOfKeysPresent++;
			}

			for(int i = 0; i < newNode.numOfKeysPresent; i++) {
				node.key.remove(newNode.key.get(i));
				node.recordId.remove(newNode.recordId.get(i));
				node.numOfKeysPresent--;
			}

			if(node != root) {
				recursiveInsert(node.parent, newNode.key.get(0), newNode);
			}
			else {
				Node R = new Node();
				R.isLeafNode = false;
				R.key.add(newNode.key.get(0));
				node.parent = R;
				newNode.parent = R;
				R.pointer.add(node);
				R.pointer.add(newNode);
				R.numOfKeysPresent++;
				root = R;
				nodeCollection.add(root);
			}
		}
	}

	// --------------------------------------------------------------------------------------
	// This method restores the tree's balance that is disturbed by a new insertion

	private void recursiveInsert(Node node, String key, Node pointer) {
		if(node.numOfKeysPresent < maxNumOfKeys) {
			node.key.add(key);
			Collections.sort(node.key);
			node.numOfKeysPresent++;
			int keyPosition = node.key.indexOf(key);
			int insertionPosition = keyPosition;
			if(keyPosition < node.numOfKeysPresent - 1) {
				for(int i = keyPosition; i < node.numOfKeysPresent - 1; i++) {
					if(node.key.get(i).compareTo(node.key.get(i + 1)) != 0) {
						break;
					}
					insertionPosition++;
				}
			}
			node.pointer.add(insertionPosition + 1, pointer);
			pointer.parent = node;
		}
		else {
			Node newNode = new Node();
			newNode.isLeafNode = false;
			nodeCollection.add(newNode);

			node.key.add(key);
			Collections.sort(node.key);
			node.numOfKeysPresent++;
			int keyPosition = node.key.indexOf(key);
			int insertionPosition = keyPosition;
			if(keyPosition < node.numOfKeysPresent - 1) {
				for(int i = keyPosition; i < node.numOfKeysPresent - 1; i++) {
					if(node.key.get(i).compareTo(node.key.get(i + 1)) != 0) {
						break;
					}
					insertionPosition++;
				}
			}
			node.pointer.add(insertionPosition + 1, pointer);
			pointer.parent = node;

			for(int i = maxNumOfKeys/2 + 1; i <= maxNumOfKeys; i++) {
				newNode.key.add(node.key.get(i));
				newNode.pointer.add(node.pointer.get(i));
				newNode.numOfKeysPresent++;
			}
			newNode.pointer.add(node.pointer.get(maxNumOfKeys + 1));

			for(int i = 0; i < maxNumOfKeys/2; i++) {
				node.key.remove(newNode.key.get(i));
				node.pointer.remove(newNode.pointer.get(i));
				node.numOfKeysPresent--;
			}
			node.pointer.remove(newNode.pointer.get(maxNumOfKeys/2));

			for(int i = 0; i <= maxNumOfKeys/2; i++) {
				newNode.pointer.get(i).parent = newNode;
			}

			key = node.key.get(maxNumOfKeys/2);
			node.key.remove(key);
			node.numOfKeysPresent--;

			if(node != root) {
				recursiveInsert(node.parent, key, newNode);
			}

			else {
				Node R = new Node();
				R.isLeafNode = false;
				R.key.add(key);
				node.parent = R;
				newNode.parent = R;
				R.pointer.add(node);
				R.pointer.add(newNode);
				R.numOfKeysPresent++;
				root = R;
			}
		}
	}

	// --------------------------------------------------------------------------------------
	// This method deletes a key value pair from the B+ tree index
	// and then reworks the tree balance

	public void deleteFromIndex(String key, int value) {
		Node leafNode = findInsertionNode(key);
		try {
			while(leafNode.recordId.contains(value) == false) {
				// If this node does not contain the value, then reverse gear to the previous node
				leafNode = leafNode.prevNodePointer;
			}
			deleteFromBTree(leafNode, key, value);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	// --------------------------------------------------------------------------------------
	// This method deletes the key value pair from the B+ tree index
	// and then reworks the tree balance

	public void deleteFromBTree(Node node, String key, int value) {
		int keyPosition;
		node.key.remove(key);
		node.recordId.remove(Integer.valueOf(value));
		node.numOfKeysPresent--;

		if(node == root) {
			return;
		}

		if(node.numOfKeysPresent < maxNumOfKeys/2) {
			Node nodeSibling = null;
			String vPrime = null;

			boolean isSiblingLeftNode = isSiblingLeft(node);

			if(isSiblingLeftNode == true) {
				int indexOfSibling = node.parent.pointer.indexOf(node) - 1;
				keyPosition = indexOfSibling;
				nodeSibling = node.parent.pointer.get(indexOfSibling);
				vPrime = node.parent.key.get(indexOfSibling);
			}
			else {
				int indexOfSibling = node.parent.pointer.indexOf(node) + 1;
				keyPosition = indexOfSibling - 1;
				nodeSibling = node.parent.pointer.get(indexOfSibling);
				vPrime = node.parent.key.get(indexOfSibling - 1);
			}

			//Redistribution of keys
			if(nodeSibling.numOfKeysPresent > maxNumOfKeys/2) {
				if(isSiblingLeftNode == true) {
					node.key.add(0, nodeSibling.key.get(nodeSibling.numOfKeysPresent - 1));
					node.recordId.add(0, nodeSibling.recordId.get(nodeSibling.numOfKeysPresent - 1));
					node.numOfKeysPresent++;

					nodeSibling.key.remove(nodeSibling.numOfKeysPresent - 1);
					nodeSibling.recordId.remove(nodeSibling.numOfKeysPresent - 1);
					nodeSibling.numOfKeysPresent--;

					node.parent.key.remove(keyPosition);
					node.parent.key.add(keyPosition, node.key.get(0));
				}
				else {
					node.key.add(nodeSibling.key.get(0));
					node.recordId.add(nodeSibling.recordId.get(0));
					node.numOfKeysPresent++;

					nodeSibling.key.remove(0);
					nodeSibling.recordId.remove(0);
					nodeSibling.numOfKeysPresent--;

					node.parent.key.remove(keyPosition);
					node.parent.key.add(keyPosition, nodeSibling.key.get(0));
				}
			}
			//Merging of nodes
			else {
				if(isSiblingLeftNode == true) {
					nodeSibling.nextNodePointer = node.nextNodePointer;
					if(node.nextNodePointer != null) {
						node.nextNodePointer.prevNodePointer = nodeSibling;
					}
					for(int i = 0; i < node.numOfKeysPresent; i++) {
						nodeSibling.key.add(node.key.get(i));
						nodeSibling.recordId.add(node.recordId.get(i));
						nodeSibling.numOfKeysPresent++;
					}
					recursiveDelete(node.parent, vPrime, node);
					nodeCollection.remove(node);
					node = null;

				}
				else {
					node.nextNodePointer = nodeSibling.nextNodePointer;
					if(nodeSibling.nextNodePointer != null) {
						nodeSibling.nextNodePointer.prevNodePointer = node;
					}
					for(int i = 0; i < nodeSibling.numOfKeysPresent; i++) {
						node.key.add(nodeSibling.key.get(i));
						node.recordId.add(nodeSibling.recordId.get(i));
						node.numOfKeysPresent++;
					}
					recursiveDelete(node.parent, vPrime, nodeSibling);
					nodeCollection.remove(nodeSibling);
					nodeSibling = null;
				}
			}
		}
	}

	// --------------------------------------------------------------------------------------
	// This method restores the tree's balance that is disturbed by the deletion

	private void recursiveDelete(Node node, String key, Node pointer) {
		node.key.remove(key);
		node.pointer.remove(pointer);
		node.numOfKeysPresent--;

		if(node == root) {	
			if(node.pointer.size() == 1) {
				root = node.pointer.get(0);
				nodeCollection.remove(node);
				node = null;
			}
			else {
				return;
			}
		}
		else if(node.numOfKeysPresent < maxNumOfKeys/2) {
			Node nodeSibling = null;
			String vPrime = null;

			boolean isSiblingLeftNode = isSiblingLeft(node);

			if(isSiblingLeftNode == true) {
				int indexOfSibling = node.parent.pointer.indexOf(node) - 1;
				nodeSibling = node.parent.pointer.get(indexOfSibling);
				vPrime = node.parent.key.get(indexOfSibling);
			}
			else {
				int indexOfSibling = node.parent.pointer.indexOf(node) + 1;
				nodeSibling = node.parent.pointer.get(indexOfSibling);
				vPrime = node.parent.key.get(indexOfSibling - 1);
			}
			if(nodeSibling.numOfKeysPresent > maxNumOfKeys/2) {
				if(isSiblingLeftNode == true) {
					node.key.add(0, vPrime);
					node.pointer.add(0, nodeSibling.pointer.get(nodeSibling.numOfKeysPresent));
					nodeSibling.pointer.get(nodeSibling.numOfKeysPresent).parent = node;
					node.numOfKeysPresent++;

					int keyPosition = node.parent.key.indexOf(vPrime);
					node.parent.key.remove(keyPosition);
					node.parent.key.add(keyPosition, nodeSibling.key.get(nodeSibling.numOfKeysPresent - 1));

					nodeSibling.key.remove(nodeSibling.numOfKeysPresent - 1);
					nodeSibling.pointer.remove(nodeSibling.numOfKeysPresent);
					nodeSibling.numOfKeysPresent--;
				}
				else {
					node.key.add(vPrime);
					node.pointer.add(nodeSibling.pointer.get(0));
					nodeSibling.pointer.get(0).parent = node;
					node.numOfKeysPresent++;

					int keyPosition = node.parent.key.indexOf(vPrime);
					node.parent.key.remove(keyPosition);
					node.parent.key.add(keyPosition, nodeSibling.key.get(0));

					nodeSibling.key.remove(0);
					nodeSibling.pointer.remove(0);
					nodeSibling.numOfKeysPresent--;
				}
			}
			else {
				if(isSiblingLeftNode == true) {
					nodeSibling.key.add(vPrime);
					nodeSibling.numOfKeysPresent++;

					for(int i = 0; i < node.numOfKeysPresent; i++) {
						nodeSibling.key.add(node.key.get(i));
						nodeSibling.numOfKeysPresent++;
					}

					for(int i = 0; i < node.pointer.size(); i++) {
						nodeSibling.pointer.add(node.pointer.get(i));
						node.pointer.get(i).parent = nodeSibling;
					}
					recursiveDelete(node.parent, vPrime, node);
					nodeCollection.remove(node);
					node = null;
				}
				else {
					node.key.add(vPrime);
					node.numOfKeysPresent++;

					for(int i = 0; i < nodeSibling.numOfKeysPresent; i++) {
						node.key.add(nodeSibling.key.get(i));
						node.numOfKeysPresent++;
					}

					for(int i = 0; i < nodeSibling.pointer.size(); i++) {
						node.pointer.add(nodeSibling.pointer.get(i));
						nodeSibling.pointer.get(i).parent = node;
					}
					recursiveDelete(node.parent, vPrime, nodeSibling);
					nodeCollection.remove(nodeSibling);
					nodeSibling = null;
				}
			}
		}

	}

	// --------------------------------------------------------------------------------------
	// This method finds the sibling with more keys

	private boolean isSiblingLeft(Node node) {
		int leftSiblingIndex = node.parent.pointer.indexOf(node) - 1;
		int rightSiblingIndex = leftSiblingIndex + 2;

		int leftSiblingKeys = -1;
		int rightSiblingKeys = -1;

		if(leftSiblingIndex > -1) {
			leftSiblingKeys = node.parent.pointer.get(leftSiblingIndex).numOfKeysPresent;
		}

		if(rightSiblingIndex <= node.parent.numOfKeysPresent) {
			rightSiblingKeys = node.parent.pointer.get(rightSiblingIndex).numOfKeysPresent;
		}

		if(leftSiblingKeys > rightSiblingKeys) {
			return true;
		}

		else {
			return false;
		}
	}

	// --------------------------------------------------------------------------------------
	// This method writes the index contents to disk

	public void dumpIndex() throws IOException {
		ObjectOutputStream out = null;

		try {
			out = new ObjectOutputStream(new BufferedOutputStream(
					new FileOutputStream(fileName + indexName)));
			out.writeObject(root);
			out.writeUTF(column);
			out.writeObject(nodeCollection);
		}
		finally {
			out.close();
		}
	}

	// --------------------------------------------------------------------------------------
	// This method is used to walk through the index and print the index contents

	public String viewIndex() {
		// The second parameter specifies the number of tabs needed to print
		String printString = "Index " + indexName + " over column " + column + "\n\n";
		printString = printString.concat(inorderTreeWalk(root, 0));
		return printString;
	}

	// --------------------------------------------------------------------------------------
	// This method does an inorder traversal of the B+ tree and prints the contents

	private String inorderTreeWalk(Node node, int tabRepetition) {
		if(node.isLeafNode == false) {
			String printString = "";
			for(int i = 0; i <= node.numOfKeysPresent; i++) {
				printString = printString.concat(inorderTreeWalk(node.pointer.get(i), tabRepetition + 1));
				if(i < node.numOfKeysPresent) {
					for(int j = 0; j < tabRepetition; j++) {
						System.out.print("\t");
						printString = printString.concat("\t");
					}
					System.out.println(node.key.get(i));
					printString = printString.concat(node.key.get(i) + "\n");
				}
			}
			return printString;
		}
		else {
			String printString = "";
			for(int i = 0; i < node.numOfKeysPresent; i++) {
				for(int j = 0; j < tabRepetition; j++) {
					System.out.print("\t");
					printString = printString.concat("\t");
				}
				System.out.println(node.key.get(i) + " " + node.recordId.get(i));
				printString = printString.concat(node.key.get(i) + " " + node.recordId.get(i) + "\n");
			}
			return printString;
		}
	}

	// --------------------------------------------------------------------------------------
	// Returns an iterator over the records that have this index's column attribute with the
	// value set to the parameter 'key'

	public Iterator<Integer> iterator(String key) {
		if(key.isEmpty() == false) {
			return new IndexIterator(key);
		}
		return null;
	}

	// --------------------------------------------------------------------------------------
	// A private class that implements the iterator methods to iterate over the file's records

	private class IndexIterator implements Iterator<Integer> {

		private Node leafNode =  null;
		private boolean nextAlreadyCalled;
		private String key;
		private int begin;
		private int flag;
		private int recordId;
		DataFile fileObj;

		public IndexIterator(String key) {
			leafNode = findInsertionNode(key);
			begin = -1;
			nextAlreadyCalled = true;
			flag = -1;
			this.key = key;
			fileObj = DataManager.getDataFile(fileName);

			if(fileObj.iteratorExists == false) {
				fileObj.iteratorExists = true;
			}

			else if(fileObj.fileModified == true) {
				fileObj.fileModified = false;
			}

			else {
				fileObj.excp = true;
			}

			while(true) {
				Node temp = leafNode.prevNodePointer;
				if(temp != null && 
						temp.key.get(temp.numOfKeysPresent - 1).compareTo(key) == 0) {
					leafNode = temp;
				}
				else {
					break;
				}
			}

			for(int i = 0; i < leafNode.numOfKeysPresent; i++) {
				if(leafNode.key.get(i).compareTo(key) == 0) {
					begin = i;
					break;
				}
			}
		}

		public boolean hasNext() {
			if(nextAlreadyCalled == true) {
				if(begin > -1) {
					nextAlreadyCalled = false;
					return true;
				}
				fileObj.iteratorExists = false;
				fileObj.fileModified = false;
				fileObj.excp = false;
				return false;
			}
			else {
				return true;
			}
		}

		public Integer next() {
			if(begin != -1) {
				fileObj.fileModified = false;
				flag = 0;
				recordId = leafNode.recordId.get(begin++);
				if(begin == leafNode.numOfKeysPresent) {
					leafNode = leafNode.nextNodePointer;
					if(leafNode == null) {
						begin = -1;
					}
					else if(leafNode.key.get(0).compareTo(key) != 0) {
						begin = -1;
					}
					else {
						begin = 0;
					}
				}
				else if(leafNode.key.get(begin).compareTo(key) != 0) {
					begin = -1;
				}
				nextAlreadyCalled = true;
				return recordId;
			}
			else {
				throw new NoSuchElementException();
			}
		}

		public void remove() {
			if (flag != -1) {
				fileObj.fileModified = true;
				ArrayList<Index> indexCollection = fileObj.getIndexCollection();
				Index indexObj = null;

				for(int i = 0; i < indexCollection.size(); i++) {
					indexObj = indexCollection.get(i);
					String keyToDelete = fileObj.getRecord(recordId).get(indexObj.getColumn());
					indexObj.deleteFromIndex(keyToDelete, recordId);
				}

				leafNode = findInsertionNode(key);
				while(true) {
					Node temp = leafNode.prevNodePointer;
					if(temp != null && 
							temp.key.get(temp.numOfKeysPresent - 1).compareTo(key) == 0) {
						leafNode = temp;
					}
					else {
						break;
					}
				}
				for(int i = 0; i < leafNode.numOfKeysPresent; i++) {
					if(leafNode.key.get(i).compareTo(key) == 0) {
						begin = i;
						break;
					}
				}
				fileObj.deleteRecord(recordId);
				flag = -1;
				indexObj = null;
				indexCollection = null;
				
				if(fileObj.excp == true) {
					throw new ConcurrentModificationException();
				}
			}
			else {
				throw new IllegalStateException();
			}
		}
	}
}