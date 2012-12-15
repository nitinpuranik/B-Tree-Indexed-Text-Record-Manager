package database;

import database.helperClasses.Node;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public class DataFile {

	// This is a collection that will contain the mapping between recordIds and their corresponding records
	private Map<Integer, Map<String, String>> recordCollection;

	// A map that contains the set of column names and their maximum permissible character length
	private Map<String, Integer> descriptor;

	// This is a collection of indexes over this file
	private ArrayList<Index> indexCollection;

	// A string that contains the name of the DataFile object
	private String fileName;
	
	// A variable to keep track of the maximum record Id
	private int maxRecId;

	// Booleans that tell if there are multiple index
	// iterators for this file
	public boolean iteratorExists;
	public boolean fileModified;
	public boolean excp;

	// --------------------------------------------------------------------------------------
	// The constructor method for the DataFile class
	// This constructor is called when creating a new DataFile object

	public DataFile(String fileName, Map<String, Integer> descriptor) {
		recordCollection = new HashMap<Integer, Map<String, String>>();
		indexCollection = new ArrayList<Index>();
		this.descriptor = descriptor;
		this.fileName = fileName;
		iteratorExists = false;
		fileModified = false;
		excp = false;
		maxRecId = -1;
	}

	// --------------------------------------------------------------------------------------
	// Another constructor for the DataFile class
	// This constructor is called when restoring a DataFile object

	public DataFile(String fileName, Map<String, Integer> descriptor,
			Map<Integer, Map<String, String>> recordCollection, int maxRecId) {
		this.fileName = fileName;
		this.descriptor = descriptor;
		this.recordCollection = recordCollection;
		this.maxRecId = maxRecId;
	}

	// --------------------------------------------------------------------------------------
	// Getter for fileName

	public String getFileName() {
		return fileName;
	}

	// --------------------------------------------------------------------------------------
	// Getter for indexCollection

	public ArrayList<Index> getIndexCollection() {
		return indexCollection;
	}

	// --------------------------------------------------------------------------------------
	// Getter for recordCollection

	public Map<Integer, Map<String, String>> getRecordCollection() {
		return recordCollection;
	}

	// --------------------------------------------------------------------------------------
	// Getter for retrieving a record from recordCollection

	public Map<String, String> getRecord(int recordId) {
		return recordCollection.get(recordId);
	}

	// --------------------------------------------------------------------------------------
	// Delete the record with the specified key from the record collection

	public void deleteRecord(int key) {
		recordCollection.remove(Integer.valueOf(key));
	}

	// --------------------------------------------------------------------------------------
	// This method returns the Index object specified by indexName if there exists one. If not,
	// null is returned

	public Index getIndex(String indexName) {
		if(indexCollection != null) {
			Iterator<Index> indexIt = indexCollection.iterator();
			while(indexIt.hasNext()) {
				Index indexObj = indexIt.next();
				if(indexObj.getIndexName().compareTo(indexName) == 0) {
					return indexObj;
				}
			}
		}
		return null;
	}

	// --------------------------------------------------------------------------------------
	// This is the method for creating a new index for the file over the specified column

	public Index createIndex(String indexName, String column) {
		// Check if the column name is valid
		if(descriptor.containsKey(column) == false) {
			throw new IllegalArgumentException("There is no column named \"" +
					column + "\" in the file \"" + fileName + "\"");
		}

		// Check if the column length is less than 25 characters
		if(column.length() > 25) {
			throw new IllegalArgumentException("The column name needs to be " +
			"less than 25 characters");
		}

		// Check if the index already exists
		if(getIndex(indexName) != null) {
			throw new IllegalArgumentException("Index \"" + indexName +
			"\" already exists in memory");
		}

		Index indexObj = new Index(indexName, column, fileName);
		FileIterator fit = new FileIterator();
		while(fit.hasNext()) {
			Map<String, String> record = recordCollection.get(fit.next());
			indexObj.insertIntoIndex(record.get(column), fit.next());
		}

		if(indexCollection == null) {
			indexCollection = new ArrayList<Index>();
		}
		indexCollection.add(indexObj);
		return indexObj;
	}

	// --------------------------------------------------------------------------------------
	// This method inserts a new record into the file and then updates all the indexes over
	// the record's columns

	public int insertRecord(Map<String, String> record) {
		Iterator<String> it = record.keySet().iterator();
		while(it.hasNext()) {
			String columnName = it.next();
			if(descriptor.containsKey(columnName) == false) {
				throw new IllegalArgumentException("The record contains an invalid column");
			}

			if(descriptor.get(columnName) < record.get(columnName).length()) {
				throw new IllegalArgumentException("Record value \"" + record.get(columnName) +
						"\" is greater than the specified " + descriptor.get(columnName) + " characters");
			}
		}
		recordCollection.put(++maxRecId, record);
		Iterator<Index> indexIt = indexCollection.iterator();
		while(indexIt.hasNext()) {
			Index indexObj = indexIt.next();
			indexObj.insertIntoIndex(record.get(indexObj.getColumn()), maxRecId);
		}
		return (maxRecId);
	}

	// --------------------------------------------------------------------------------------
	// Dump the file contents to disk. If there is an existing file already, then overwrite
	// the existing file with the current file contents

	public void dumpFile() throws IOException {
		ObjectOutputStream out = null;
		try {
			out = new ObjectOutputStream(new BufferedOutputStream(
					new FileOutputStream(getFileName())));
			out.writeObject(recordCollection);
			out.writeObject(descriptor);
			out.writeInt(maxRecId);
		}
		finally {
			out.close();
		}
	}

	// --------------------------------------------------------------------------------------
	// View all the records that this file contains

	public String viewFile() {
		String printString = "";
		FileIterator fit = new FileIterator();
		while(fit.hasNext()) {
			Map<String, String> record = recordCollection.get(fit.next());
			Iterator<String> mapIt = record.keySet().iterator();
			System.out.println(fit.next() + ":");
			printString = printString.concat(fit.next() + ":" + "\n");

			while(mapIt.hasNext()) {
				String key = mapIt.next();
				System.out.println("\t" + key + ": " + record.get(key));
				printString = printString.concat("\t" + key + ": " + 
						record.get(key) + "\n");
			}
		}
		return printString;
	}

	// --------------------------------------------------------------------------------------
	// Remove the file object and its contents from memory and if there is such a file on disk,
	// then remove from disk too

	public void dropFile() {
		recordCollection = null;
		descriptor = null;

		Iterator<Index> indexIt = indexCollection.iterator();
		File fileObj = null;

		while(indexIt.hasNext()) {
			Index indexObj = indexIt.next();
			String indexFile = indexObj.getFileName() + indexObj.getIndexName();
			fileObj = new File(indexFile);
			// Delete the index files from the disk if such a file exists. Else
			// do nothing
			if(fileObj.exists()) {
				fileObj.delete();
			}
			indexObj = null;
		}

		indexCollection = null;

		fileObj = new File(fileName);
		if(fileObj.exists()) {
			fileObj.delete();
		}

		DataManager.setFileCollection(fileName);
		fileName = null;
		fileObj = null;
	}

	// --------------------------------------------------------------------------------------
	// Restore the file's index specified by indexName from the disk

	public Index restoreIndex(String indexName) throws IOException, ClassNotFoundException {
		//Check if the index exists in memory
		if(getIndex(indexName) != null) {
			throw new IllegalArgumentException("Index \"" + indexName +
			"\" already exists in memory");			
		}

		if(new File(fileName + indexName).exists() == false) {
			throw new IllegalArgumentException("There is no such file named \"" +
					indexName + "\" on disk");
		}

		ObjectInputStream in = null;
		Index indexObj = null;

		try {
			in = new ObjectInputStream(new BufferedInputStream(
					new FileInputStream(fileName + indexName)));
			Node root = (Node) in.readObject();
			String column = in.readUTF();
			@SuppressWarnings("unchecked")
			ArrayList<Node> nodeCollection = (ArrayList<Node>) in.readObject();

			indexObj = new Index(indexName, column, fileName,
					root, nodeCollection);

			if(indexCollection == null) {
				indexCollection = new ArrayList<Index>();
			}

			indexCollection.add(indexObj);
		}
		finally {
			in.close();
		}	
		return indexObj;
	}

	// --------------------------------------------------------------------------------------
	// Drop the index over the file specified by indexName

	public void dropIndex(String indexName) {
		// Check if there exists such an index in memory and if yes, 
		// remove from indexCollection
		Index indexObj = getIndex(indexName);
		if(indexObj != null) {
			indexCollection.remove(indexObj);
		}

		// Check if there exists a file on disk for the given index. If yes.
		// delete the file
		File fileObj = new File(fileName + indexName);

		if(fileObj.exists()) {
			fileObj.delete();
		}

		fileObj = null;
		indexObj = null;
	}

	// --------------------------------------------------------------------------------------
	// Return an iterator over the recordCollection for this DataFile object

	public Iterator<Integer> iterator() {
		return new FileIterator();
	}

	// --------------------------------------------------------------------------------------
	// A private class that implements the iterator methods to iterate over the file's records

	private class FileIterator implements Iterator<Integer> {

		private int numRecords;
		private int nextRecordId;
		int[] keys = new int[recordCollection.size()];
		int keyCount;
		int flag;

		public FileIterator() {
			for (int i : recordCollection.keySet()) {
				keys[keyCount++] = i;
			}
			nextRecordId = -1;
		}

		public boolean hasNext() {
			if (numRecords < recordCollection.size()) {
				nextRecordId = nextRecordId + 1;
				numRecords = numRecords + 1;
				return true;
			}
			else {
				return false;
			}
		}

		public Integer next() {
			try {
				flag = 0;
				return keys[nextRecordId];
			}
			catch(Exception ex) {
				throw new NoSuchElementException();
			}
		}

		//Get the record. Get the value for the column on which the index is built
		public void remove() {
			if (flag != -1) {
				Map<String, String> record = recordCollection.get(keys[nextRecordId]);
				Index indexObj = null;

				for(int i = 0; i < indexCollection.size(); i++) {
					indexObj = indexCollection.get(i);
					String key = record.get(indexObj.getColumn());
					indexObj.deleteFromIndex(key, keys[nextRecordId]);
				}

				indexObj = null;
				recordCollection.remove(keys[nextRecordId]);

				flag = -1;
				numRecords = numRecords - 1;	
			}
			else {
				throw new IllegalStateException();
			}
		}
	}
}