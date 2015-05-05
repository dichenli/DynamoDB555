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
public class Misspell {
	
	/** The name. */
	@PrimaryKey
	String wrong;
	
	/** The frontier. */
	String right;
	
	/**
	 * Instantiates a new URL frontier.
	 *
	 * @param wrong the wrong
	 */
	public Misspell(String wrong){
		this.wrong = wrong;
	}
	
	/**
	 * Instantiates a new URL frontier.
	 */
	private Misspell(){
		
	}
	
	/**
	 * Adds the url to last.
	 *
	 * @param right the right
	 */
	public void addRight(String right){
		this.right = right;
	}
	

	/**
	 * Gets the right.
	 *
	 * @return the right
	 */
	public String getRight(){
		return right;
	}

}

