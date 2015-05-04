package SearchUtils;

import java.nio.ByteBuffer;
import java.util.List;

import DynamoDB.QueryRecord;

public class DocResult {
	private static final int BASE = 30;
	private static final int WINDOW = 5;
	
	private static final double W_POSITION = 0.5;
	private static final double W_PAGERANK = 0.3;
	private static final double W_ANCHOR = 0;
	private static final double W_TFIDF = 0.2;
	private static final double W_CLICK = 0.2;
	
	String query;
	ByteBuffer id;
	double[] wordtf;
	List<Integer>[] positions;
	List<Double> idflist;
	int[] windowlist;
	String url;
	int size;
	AnchorResult[] anchors;
	int clickcount;
	
	// different factors
	double tfidf;
	int count = 0;
	double positionScore = 0;
	double anchorScore = 0;
	double pageRank;
	double finalScore;

	public DocResult(String query, ByteBuffer id, int size, int[] windowlist, List<Double> idflist) {
		this.query = query;
		this.id = id;
		this.size = size;
		positions = (List<Integer>[])new List[size];
		this.windowlist = windowlist;
		this.wordtf = new double[size];
		for(int i=0;i<size;i++) wordtf[i] = 0;
		this.idflist = idflist;
		anchors = new AnchorResult[size];
	}

	public boolean containsAll() {
		return size == count;
	}
	
	public void setClickScore(){
		QueryRecord qr = QueryRecord.load(query, id);
		if(qr != null) clickcount = qr.getCount();
	}

	public void setPositionList(int index, List<Integer> position) {
		positions[index] = position;
		count++;
	}
	
	public void setTF(int index, double tf){
		wordtf[index] = tf;
	}
	
	public void setAnchor(int wordindex, int type){
		anchors[wordindex].setType(type);
	}

	public void setFinalScore(double finalScore) {
		this.finalScore = finalScore;
	}
	
	public int getClickCount(){
		return clickcount;
	}

	public List<Integer>[] getPositions() {
		return positions;
	}
	
	public double getPositionScore(){
		return positionScore;
	}
	
	public ByteBuffer getDocID(){
		return id;
	}

	// calculate position score
	public void setPositionScore() {
		for (int i = 0; i < positions.length - 1; i++) {
			int score = 0, j = 0, k = 0, dis;
			boolean firstTime = true;
			List<Integer> word1 = positions[i];
			List<Integer> word2 = positions[i+1];

			// iterate through the two position lists, and both start from index
			// 0
			while (j < word1.size() && k < word2.size()) {
				dis = word2.get(k) - word1.get(j);
				/*
				 * if it's "word2 ... word1" move the pointer of word2 to next
				 */
				if (dis <= 0) {
					k++;
				}
				else if (dis > 0 && dis <= windowlist[i]) {
					if (firstTime) {
						score = BASE;
						firstTime = false;
					} else {
						score += 1;
					}
					j++;
				}
				/*
				 * if it's "word1 ... word2" but their distance is greater than
				 * window move the pointer of word1 to next
				 */
				else {
					j++;

				}
			}
			positionScore += score;
		}
		if(size != 1) positionScore = positionScore/((size-1)*BASE);
	}

	// calculate tf score
	public void setTFScore() {
		tfidf = 0;
		for(int i=0;i<size;i++){
			tfidf += wordtf[i]*idflist.get(i);
		}
	}
	
	// calculate anchor score
	public void setAnchorScore() {
		
	}
	
	// calculate pageRank score
	public void setPageRank(){
//		pageRank = PageRank.load(id).getRank();
	}
	
	public void calculateScore(){
		setPositionScore();
		setPageRank();
		setAnchorScore();
		setTFScore();
		setClickScore();
		finalScore = W_POSITION*positionScore + W_PAGERANK*pageRank/280.0 + W_ANCHOR*anchorScore + W_TFIDF*tfidf;
	}
	
	public double getPageRank() {
		return pageRank;
	}

	public double getTF(int index) {
		return wordtf[index];
	}

	public double getFinalScore() {
		return finalScore;
	}

	public int compareTo(Object other) {
		if (this.finalScore == ((DocResult) other).finalScore)
			return 0;
		else if (this.finalScore > ((DocResult) other).finalScore)
			return 1;
		else
			return -1;
	}
}
