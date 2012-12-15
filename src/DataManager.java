package database;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

public class DataManager {

	// An ArrayList collection of DataFile objects
	private static ArrayList<DataFile> fileCollection = new ArrayList<DataFile>();

	// --------------------------------------------------------------------------------------
	// Removes the DataFile object specified by fileName from fileCollection

	public static void setFileCollection(String fileName) {
		Iterator<DataFile> fileIt = fileCollection.iterator();
		while(fileIt.hasNext()) {
			if(fileIt.next().getFileName().compareTo(fileName) == 0) {
				fileIt.remove();
			}
		}
	}

	// --------------------------------------------------------------------------------------
	// Returns a DataFile object specified by fileName

	public static DataFile getDataFile(String fileName) {
		if(fileCollection != null) {
			Iterator<DataFile> fileIt = fileCollection.iterator();
			while(fileIt.hasNext()) {
				DataFile fileObj = fileIt.next();
				if(fileObj.getFileName().compareTo(fileName) == 0) {
					return fileObj;
				}
			}
		}
		return null;
	}

	// --------------------------------------------------------------------------------------
	// Creates a new DataFile object

	public static DataFile createFile(String fileName, Map<String, Integer> descriptor) {
		if (DataManager.getDataFile(fileName) != null) {
			throw new IllegalArgumentException("File \"" + fileName + 
			"\" already exists in memory");
		}

		DataFile fileObj = new DataFile(fileName, descriptor);
		if(fileCollection == null) {
			fileCollection = new ArrayList<DataFile>();
		}
		fileCollection.add(fileObj);
		return fileObj;
	}

	// --------------------------------------------------------------------------------------
	// Restores a file specified by fileName from disk

	public static DataFile restoreFile(String fileName) 
	throws IOException, ClassNotFoundException {
		//Check if the file exists in memory
		if(getDataFile(fileName) != null) {
			throw new IllegalArgumentException("File \"" + fileName + 
			"\" already exists in memory");
		}

		//Check if the file exists on the disk
		if(new File(fileName).exists() == false) {
			throw new IllegalArgumentException("There is no such file named \"" +
					fileName + "\" on disk");
		}

		//Get the de-serialized file from disk into a DataFile object
		ObjectInputStream in = null;
		DataFile fileObj = null;

		try {
			in = new ObjectInputStream(new BufferedInputStream(
					new FileInputStream(fileName)));

			@SuppressWarnings("unchecked")
			Map<Integer, Map<String, String>> recordCollection =
				(Map<Integer, Map<String,String>>) in.readObject();

			@SuppressWarnings("unchecked")
			Map<String, Integer> descriptor =
				(Map<String, Integer>) in.readObject();
			
			int maxRecId = in.readInt();

			fileObj = new DataFile(fileName, descriptor, recordCollection, maxRecId);
			if(fileCollection == null) {
				fileCollection = new ArrayList<DataFile>();
			}
			fileCollection.add(fileObj);
			
		}
		finally {
			in.close();
		}
		return fileObj;
	}

	// --------------------------------------------------------------------------------------
	// Prints the passed record parameter

	public static String print(Map<String, String> record) {
		String printString = "";
		Iterator<String> mapIt = record.keySet().iterator();
		while(mapIt.hasNext()) {
			String key = mapIt.next();
			if(record.get(key) != null) {
				System.out.println(key + ": " + record.get(key));
				printString = printString.concat(key + ": " + record.get(key) + "\n");
			}
		}
		return printString;
	}

	// --------------------------------------------------------------------------------------
	// Exits the system after saving memory contents
	public static void exit() throws IOException {
    if(fileCollection != null) {
        Iterator<DataFile> fileIt = fileCollection.iterator();
        while(fileIt.hasNext()) {
           DataFile fileObj = fileIt.next();
           if(fileObj.getIndexCollection() != null) {
              Iterator<Index> indexIt = fileObj.getIndexCollection().iterator();
              while(indexIt.hasNext()) {
                 Index indexObj = indexIt.next();
                 indexObj.dumpIndex();
                 indexObj = null;
              }
           }
           fileObj.dumpFile();
           fileObj = null;
        }
        fileCollection = null;
    }
	}
}
