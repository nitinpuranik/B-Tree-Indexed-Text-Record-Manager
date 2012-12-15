package database.helperClasses;

import java.io.Serializable;
import java.util.ArrayList;

public class Node implements Serializable {

	private static final long serialVersionUID = 1L;

	// A boolean variable that lets us know if it is a leaf node or not.
	public boolean isLeafNode;
	
	// The maximum number of string keys that the node can contain. Set to 8 as asked in the document.
	public ArrayList<String> key;

	// The pointers corresponding to the keys in a given node. Relevant only for internal nodes
	public ArrayList<Node> pointer;
	
	// The last pointer of leaf nodes to point to the next node
	public Node nextNodePointer;
	
	// The pointer to the previous node
	public Node prevNodePointer;

	// The record Ids of leaf nodes. This would be relevant only if the node is a leaf node.
	public ArrayList<Integer> recordId;

	// The parent of this node. Will be null if this is a root node
	public Node parent;
	
	// A variable that records how many keys the node contains
	public int numOfKeysPresent;
	
	// --------------------------------------------------------------------------------------
	// The constructor for the Node class

	public Node() {
		isLeafNode = true;
		key = new ArrayList<String>();
		pointer = new ArrayList<Node>();
		recordId = new ArrayList<Integer>();
		parent = null;
		nextNodePointer = null;
		prevNodePointer = null;
	}
}
