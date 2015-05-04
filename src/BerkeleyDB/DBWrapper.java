package BerkeleyDB;

import java.io.File;
import java.io.IOException;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;




public class DBWrapper {

	/** The env directory. */
	private static String envDirectory = null;
	
	/** The my env. */
	private static Environment myEnv;
	
	/** The store. */
	private static EntityStore store;

	
	/** The dBDictionary. */
	public PrimaryIndex<String, DBDictionary> dBDictionary;
	
	/** The misspell file. */
	public PrimaryIndex<String, Misspell> misspell;
	
	
	/** The frontier. */
	public String dict = "dBDictionary";
	
	
	/**
	 * Instantiates a new DB wrapper.
	 *
	 * @param envDirectory the env directory
	 */
	public DBWrapper(String input_envDirectory){
		if(!input_envDirectory.endsWith("/")) input_envDirectory += "/";
		
		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setAllowCreate(true);
		StoreConfig stConfig = new StoreConfig();
		stConfig.setAllowCreate(true); 
		envDirectory = input_envDirectory;
		File f = new File(envDirectory);
		
		try {
			System.out.println(f.getCanonicalPath());
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		if(!f.exists()) f.mkdir();
		if(!f.isDirectory()) throw new IllegalArgumentException();
		myEnv = new Environment(f, envConfig);
		store = new EntityStore(myEnv, "EntityStore", stConfig);

		
		
		dBDictionary = store.getPrimaryIndex(String.class, DBDictionary.class);
		misspell = store.getPrimaryIndex(String.class, Misspell.class);
		
	}
	
	/**
	 * Gets the store.
	 *
	 * @return the store
	 */
	public EntityStore getStore(){
		return store;
	}
	
	/**
	 * Gets the env.
	 *
	 * @return the env
	 */
	public Environment getEnv(){
		return myEnv;
	}
	
	public void initializeDictionary(){
		DBDictionary dic = new DBDictionary(dict);
		dBDictionary.put(dic);
	}
	
	public void addWord(String word){
		DBDictionary dic = dBDictionary.get(dict);
//		System.out.println("dic is "+dic+"\nand the word is "+word);
		dic.addWord(word);
		dBDictionary.put(dic);
	}
	
	public void addMisspell(String wrong, String correct){
		Misspell misSpell = new Misspell(wrong);
		misSpell.addRight(correct);
		misspell.put(misSpell);
	}
	
	public boolean containsWord(String word){
		DBDictionary dic = dBDictionary.get(dict);
		System.out.println(dic == null);
		return dic.containsWord(word);
	}
	
	public boolean containsMisspell(String wrong){
		Misspell miss = misspell.get(wrong);
		if(miss == null){
			return false;
		}
		else{
			return true;
		}
	}
	
	public String getRight(String wrong){
		Misspell miss = misspell.get(wrong);
		String right = miss.getRight();
		return right;
	}
	
	
	public void closeEnv() {
		if(store != null){
			try{
				store.close();
			} catch(DatabaseException dbe){
				
			}
		}
		
		
		if (myEnv != null) {
			try {
				myEnv.close();
			} catch(DatabaseException dbe) {
				System.err.println("Error closing environment" +
				dbe.toString());
			}
		}
	}
	
	
}
