package SearchUtils;

import java.util.List;

import Utils.BinaryUtils;

public class SearchResult {
	String url;
	String decimalID;
	String title;
	List<String> wordList;
	
	public SearchResult(String url, byte[] docID, String title, List<String> wordList){
		this.url = url;
		this.decimalID = BinaryUtils.byteArrayToDecimalString(docID);
		this.wordList = wordList;
	}

	public String getUrl(){
		return url;
	}
	
	/**
	 * get id in decimal String
	 * @return
	 */
	public String getID() {
		return decimalID;
	}
	
	public String getTitle() {
		return title;
	}
	
	public List<String> getWordList() {
		return wordList;
	}
	
	/**
	 * return a string representation of a list of words, separated by space
	 * @return
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
