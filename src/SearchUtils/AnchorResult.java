package SearchUtils;

public class AnchorResult {
	
	//type: 0:url, 1:meta, 2:anchor, 3:title

	String word;
	int[] typecount;
	
	public AnchorResult(){
		typecount = new int[4];
	}
	
	public AnchorResult(String word){
		this.word = word;
		typecount = new int[4];
	}
	
	public void setType(int type){
		typecount[type-1]++;
	}
}
