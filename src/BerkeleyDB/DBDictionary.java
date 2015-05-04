package BerkeleyDB;

import java.util.LinkedList;
import java.util.Queue;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;


// TODO: Auto-generated Javadoc
/**
 * The Class URLFrontier.
 */
@Entity
public class DBDictionary {
	
	/** The name. */
	@PrimaryKey
	String name;
	
	/** The frontier. */
	Queue<String> dictionary;
	
	/**
	 * Instantiates a new URL frontier.
	 *
	 * @param name the name
	 */
	public DBDictionary(String name){
		this.name = name;
		dictionary = new LinkedList<String>();
	}
	
	/**
	 * Instantiates a new URL frontier.
	 */
	private DBDictionary(){
		
	}
	
	/**
	 * Adds the word to last.
	 *
	 * @param word the word
	 */
	public void addWord(String word){
		dictionary.offer(word);
	}
	
	/**
	 * Contains word.
	 *
	 * @param word the word
	 * @return true, if successful
	 */
	public boolean containsWord(String word){
		if(dictionary.size()==0){
			return false;
		}
		return dictionary.contains(word);
	}
	
	/**
	 * Checks if is empty.
	 *
	 * @return true, if is empty
	 */
	public boolean isEmpty(){
		return dictionary.isEmpty();
	}
	


	
	/**
	 * Gets the frontier.
	 *
	 * @return the frontier
	 */
	public Queue<String> getFrontier(){
		return dictionary;
	}

}

