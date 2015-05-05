package SearchUtils;

import java.util.List;

import Utils.BinaryUtils;

// TODO: Auto-generated Javadoc
/**
 * The Class SearchResult.
 */
public class SearchResult {
	
	/** The url. */
	String url;
	
	/** The decimal id. */
	String decimalID;
	
	/** The title. */
	String title;
	
	/** The word list. */
	List<String> wordList;
	
	/**
	 * Instantiates a new search result.
	 *
	 * @param url the url
	 * @param docID the doc id
	 * @param title the title
	 * @param wordList the word list
	 */
	public SearchResult(String url, byte[] docID, String title, List<String> wordList){
		this.url = url;
		this.decimalID = BinaryUtils.byteArrayToDecimalString(docID);
		this.title = title;
		this.wordList = wordList;
	}

	/**
	 * Gets the url.
	 *
	 * @return the url
	 */
	public String getUrl(){
		return url;
	}
	
	/**
	 * get id in decimal String.
	 *
	 * @return the id
	 */
	public String getID() {
		return decimalID;
	}
	
	/**
	 * Gets the title.
	 *
	 * @return the title
	 */
	public String getTitle() {
		return title;
	}
	
	/**
	 * Gets the word list.
	 *
	 * @return the word list
	 */
	public List<String> getWordList() {
		return wordList;
	}
	
	/**
	 * return a string representation of a list of words, separated by space.
	 *
	 * @return the wordlist marshall
	 */
	public String getWordlistMarshall() {
		String str = "";
		for(String word : wordList) {
			str += word;
			str += " ";
		}
		return str.trim();
	}
}
