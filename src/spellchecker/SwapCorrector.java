package spellchecker;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import BerkeleyDB.DBWrapper;

// TODO: Auto-generated Javadoc
/**
 * The Class SwapCorrector.
 */
public class SwapCorrector extends Corrector {
	
	/** The dict. */
	Dictionary dict;

	DBWrapper db;
	
	/**
	 * Instantiates a new swap corrector.
	 *
	 * @param db the dict
	 */
	public SwapCorrector(DBWrapper db) {
		this.db = db;
		
	}
	
	/**
	 * Gets the correction.
	 *
	 * @param wrong the wrong
	 * @return the correction
	 */
	public String getCorrection(String wrong){
		for(int i = 0; i < wrong.length()-1; i++){
			String maybe = swap(wrong, i, i+1);
			System.out.println("the maybe word is "+maybe);
			if(db.containsWord(maybe)){
				return maybe;
			}
			
			
		}
		return wrong;
	}



	/* (non-Javadoc)
	 * @see spellchecker.Corrector#getCorrections(java.lang.String)
	 */
	@Override
	public Set<String> getCorrections(String wrong) {
		Set<String> results = new HashSet<String>();
		for(int i = 0; i < wrong.length()-1; i++){
			String maybe = swap(wrong, i, i+1);
			System.out.println(maybe);
			if(dict.isWord(maybe)){
				results.add(maybe);
			}
			
			
		}
		return results;
	}



	/**
	 * Swap.
	 *
	 * @param wrong the wrong
	 * @param i the i
	 * @param j the j
	 * @return the string
	 */
	private String swap(String wrong, int i, int j) {
		int first, second;
		if(i < j){
			first = i;
			second = j;
		}
		else{
			second = i;
			first = j;
		}
		StringBuilder result = new StringBuilder("");
		result.append(wrong.substring(0, first)).append(wrong.charAt(second)).append(wrong.substring(first+1, second)).append(wrong.charAt(first)).append(wrong.substring(second+1, wrong.length()));
		return result.toString();
	}
	


}
