package SearchUtils;

// TODO: Auto-generated Javadoc
/**
 * The Class AnchorResult.
 */
public class AnchorResult {
	
	/** The Constant W_URL. */
	private static final double W_URL = 0.5;
	
	/** The Constant W_TITLE. */
	private static final double W_TITLE = 0.5;
	
	/** The Constant W_META. */
	private static final double W_META = 0.2;
	
	/** The Constant W_ANCHOR. */
	private static final double W_ANCHOR = 0.4;
	
	/** The Constant BASE_COUNT. */
	private static final int BASE_COUNT = 1;
	
	/** The Constant BASE_SHARE. */
	private static final int BASE_SHARE = 10;
	
	//type: 0:url, 1:meta, 2:anchor, 3:title
	/** The size. */
	int size;
	
	/** The url. */
	int[] url;
	
	/** The meta. */
	int[] meta;
	
	/** The anchor. */
	int[] anchor;
	
	/** The title. */
	int[] title;
	
	/** The anchor score. */
	double anchorScore = 0;
	
	/**
	 * Instantiates a new anchor result.
	 *
	 * @param size the size
	 */
	public AnchorResult(int size){
		this.size = size;
		this.url = new int[2];
		this.meta = new int[2];
		this.anchor = new int[2];
		this.title = new int[2];
	}
	
	/**
	 * Sets the type.
	 *
	 * @param index the index
	 * @param type the type
	 */
	public void setType(int index, int type){
		switch (type){
			case 1: url[index]++;
			case 2: meta[index]++;
			case 3: anchor[index]++;
			case 4: title[index]++;
		}
	}
	
	/**
	 * Checks if is useful anchor.
	 *
	 * @return true, if is useful anchor
	 */
	public boolean isUsefulAnchor(){
		return anchorScore > 0;
	}
	
	/**
	 * Gets the anchor score.
	 *
	 * @return the anchor score
	 */
	public double getAnchorScore(){
		return anchorScore;
	}
	
	/**
	 * Sets the anchor score.
	 */
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
