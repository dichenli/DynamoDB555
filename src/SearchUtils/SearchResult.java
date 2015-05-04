package SearchUtils;

import Utils.BinaryUtils;

public class SearchResult {
	String url;
	String decimalID;
	String title;
	
	public SearchResult(String url, byte[] docID, String title){
		this.url = url;
		this.decimalID = BinaryUtils.byteArrayToDecimalString(docID);
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
}
