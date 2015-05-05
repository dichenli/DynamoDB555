
package spellchecker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import BerkeleyDB.DBWrapper;

// TODO: Auto-generated Javadoc
/**
 * The Class FileCorrector.
 */
public class FileCorrector extends Corrector {
	
	/** The correct file. */
	File correctFile;
	
	/** The freader. */
	FileReader freader;
	
	/** The breader. */
	BufferedReader breader;

	DBWrapper db;
	/**
	 * Instantiates a new file corrector.
	 *
	 * @param db the file path
	 */
	public FileCorrector(DBWrapper db){
		this.db = db;
	}
	
	/**
	 * Instantiates a new file corrector.
	 */
	public FileCorrector(){
		
	}
	
	/**
	 * Write to db.
	 */
	public void writeToDB(){
		DBWrapper db = new DBWrapper(DBdir.dir);

		Set<String> results = new HashSet<String>();
		String line;
		try {
			freader= new FileReader(correctFile);
			breader = new BufferedReader(freader);
			
			while((line=breader.readLine()) != null){
				String[] pairs = line.split(",");
				db.addMisspell(pairs[0], pairs[1]);
				
			}
			breader.close();
			freader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	
		db.closeEnv();
	}
	
	/**
	 * Contains misspell.
	 *
	 * @param wrong the wrong
	 * @return true, if successful
	 */
	public boolean containsMisspell(String wrong){
		boolean contain = db.containsMisspell(wrong);
		return contain;
	}
	
	/**
	 * Gets the correction.
	 *
	 * @param word the word
	 * @return the correction
	 */
	public String getCorrection(String word){
		String right = db.getRight(word);
		return right;
		
	}

	/* (non-Javadoc)
	 * @see spellchecker.Corrector#getCorrections(java.lang.String)
	 */
	@Override
	public Set<String> getCorrections(String wrong) {
		Set<String> results = new HashSet<String>();
		String line;
		try {
			freader= new FileReader(correctFile);
			breader = new BufferedReader(freader);
			
			while((line=breader.readLine()) != null){
				String[] pairs = line.split(",");
				if(pairs[0].equals(wrong.toLowerCase())){
					results.add(pairs[1]);
					
				}
				
			}
			breader.close();
			freader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		Set<String> fin = matchCase(wrong, results);
		
		return fin;
	}

}
