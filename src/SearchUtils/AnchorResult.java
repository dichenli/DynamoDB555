package SearchUtils;

public class AnchorResult {
	
	private static final double W_URL = 0.5;
	private static final double W_TITLE = 0.5;
	private static final double W_META = 0.2;
	private static final double W_ANCHOR = 0.4;
	
	private static final int BASE_COUNT = 1;
	private static final int BASE_SHARE = 10;
	
	//type: 0:url, 1:meta, 2:anchor, 3:title
	int size;
	int[] url;
	int[] meta;
	int[] anchor;
	int[] title;
	double anchorScore = 0;
	
	public AnchorResult(int size){
		this.size = size;
		this.url = new int[2];
		this.meta = new int[2];
		this.anchor = new int[2];
		this.title = new int[2];
	}
	
	public void setType(int index, int type){
		switch (type){
			case 1: url[index]++;
			case 2: meta[index]++;
			case 3: anchor[index]++;
			case 4: title[index]++;
		}
	}
	
	public boolean isUsefulAnchor(){
		return anchorScore > 0;
	}
	
	public double getAnchorScore(){
		return anchorScore;
	}
	
	public void setAnchorScore(){
		double urlscore = 0;
		int urlcount = 0;
		double metascore = 0;
		int metacount = 0;
		double anchorscore = 0;
		int anchorcount = 0;
		double titlescore = 0;
		int titlecount = 0;
		
		int urltotal = 0, metatotal = 0, anchortotal = 0, titletotal = 0;
		for(int i=0;i<size;i++){
			if(url[i]>0) {
				urlcount++;
				urltotal+=url[i];
			}
			if(meta[i]>0) {
				metacount++;
				metatotal+=meta[i];
			}
			if(anchor[i]>0) {
				anchorcount++;
				anchortotal+=anchor[i];
			}
			if(title[i]>0) {
				titlecount++;
				titletotal+=title[i];
			}
		}
		urlscore = urlcount*BASE_SHARE + urltotal*BASE_COUNT;
		metascore = metacount*BASE_SHARE + metatotal*BASE_COUNT;
		anchorscore = anchorcount*BASE_SHARE + anchortotal*BASE_COUNT;
		titlescore = titlecount*BASE_SHARE + titletotal*BASE_COUNT;
		anchorScore = (W_URL*urlscore + W_META*metascore + W_ANCHOR*anchorscore + W_TITLE*titlescore)/(size*BASE_SHARE);
	}
}
