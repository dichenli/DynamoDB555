package SearchUtils;

public class AnchorResult {

	String word;
	int[] typecount;
	
	public AnchorResult(String word){
		this.word = word;
	}
	
	public void setType(int type){
		typecount[type]++;
	}
}
