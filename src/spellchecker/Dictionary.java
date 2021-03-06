package spellchecker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import BerkeleyDB.DBWrapper;

// TODO: Auto-generated Javadoc
/**
 * The Class Dictionary.
 */
public class Dictionary {
	
	/** The dictionary. */
	File dictionary;
	
	/** The freader. */
	FileReader freader;
	
	/** The breader. */
	BufferedReader breader;
	
	DBWrapper db;
	
	/**
	 * Instantiates a new dictionary.
	 */
	public Dictionary(){
		
	}
	
	/**
	 * Instantiates a new dictionary.
	 *
	 * @param db the string
	 */
	public Dictionary(DBWrapper db){
		this.db = db;
		
	}
	
	/**
	 * Write to db.
	 */
	public void writeToDB(){
		DBWrapper db = new DBWrapper(DBdir.dir);

		db.initializeDictionary();
		String line;
		try {
			freader= new FileReader(dictionary);
			breader = new BufferedReader(freader);
			
			while((line=breader.readLine()) != null){
				System.out.println("@"+line+"@");
				db.addWord(line.trim());
				
			}
			breader.close();
			freader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	
		db.closeEnv();
	}

	/**
	 * Checks if is word.
	 *
	 * @param word the word
	 * @return true, if is word
	 */
	public boolean isWord(String word){

		boolean contains = db.containsWord(word);
		return contains;
	}

}
