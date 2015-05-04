package spellchecker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import BerkeleyDB.DBWrapper;

public class Dictionary {
	File dictionary;
	FileReader freader;
	BufferedReader breader;
	
	
	public Dictionary(){
		
	}
	
	public Dictionary(String string){
		dictionary = new File(string);
		
	}
	
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

	public boolean isWord(String word){
		DBWrapper db = new DBWrapper(DBdir.dir);

		return db.containsWord(word);
	}

}
