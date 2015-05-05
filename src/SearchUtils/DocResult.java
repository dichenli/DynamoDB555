package SearchUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.StringTokenizer;

import Utils.ProcessUtils;
import DynamoDB.DocURLTitle;
import DynamoDB.QueryRecord;

public class DocResult {
	private static final int BASE = 30;
	private static final int WINDOW = 3;
	
	private static final double W_POSITION_MULTIPLE = 0.5;
	private static final double W_PAGERANK_MULTIPLE = 0.2;
	private static final double W_TFIDF = 0.2;
	private static final double W_CLICK_MULTIPLE = 0.2;
	
	private static final double W_PAGERANK = 0.3;
	private static final double W_TF = 0.4;
	private static final double W_CLICK = 0.3;
	
	private static final double BASE_URL = 5;
	private static final double BASE_TITLE = 10;
	
	List<String> wordlist;
	ByteBuffer id;
	double[] wordtf;
	List<Integer>[] positions;
	List<Double> idflist;
	int[] windowlist;
	String url;
	String title;
	int size;
	AnchorResult anchors;
	int clickcount;
	
	// different factors
	double tfidf;
	int count = 0;
	int positionScore = 0;
	double anchorScore = 0;
	double pageRank;
	double pageScore;
	double finalScore;

	public DocResult(QueryInfo queryInfo, ByteBuffer id, double pageRank) {
		this.wordlist = queryInfo.wordlist;
		this.size = wordlist.size();
		this.windowlist = queryInfo.getWindowlist();
		this.idflist = queryInfo.getIDFlist();
		this.id = id;
		this.pageRank = pageRank;
		positions = (List<Integer>[])new List[size];
		this.wordtf = new double[size];
		for(int i=0;i<size;i++) wordtf[i] = 0;
	}

	public boolean containsAll() {
		return size == count;
	}
	
//	public boolean isUserfulAnchor() {
//		anchors.setAnchorScore();
//		return anchors.isUsefulAnchor();
//	}
	
	public void setClickScore(int count){
		clickcount = count;
	}

	public synchronized void setPositionList(int index, List<Integer> position) {
		positions[index] = position;
		count++;
	}
	
	public void setTF(int index, double tf){
		wordtf[index] = tf;
	}
	
//	public void setAnchor(int index, int type){
//		anchors.setType(index, type);
//	}
//	
//	public void calculateAnchor(){
//		anchorScore = anchors.getAnchorScore();
//	}

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
	public int setPositionScore() {
		
		for (int i = 0; i < positions.length - 1; i++) {
			int score = 0, j = 0, k = 0, dis;
			boolean firstTime = true;
			List<Integer> word1 = positions[i];
			List<Integer> word2 = positions[i+1];

			// iterate through the two position lists, and both start from index
			// 0
			if(word1 == null || word2 == null) return 0;
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
		return positionScore;
	}

	// calculate tf score
	public void setTFScore() {
		tfidf = 0;
		for(int i=0;i<size;i++){
			tfidf += wordtf[i]*idflist.get(i);
		}
	}
	
	public double getAnchorScore() {
		return anchorScore;
	}
	
	// calculate pageRank score
	public void setPageScore(){
		pageScore = Math.log(pageRank + 1);
		pageScore = pageScore/5;
	}
	
	public void firstScore(int maxClickCount){
		setPageScore();
//		calculateAnchor();
		setTFScore();
		if(maxClickCount == 0) maxClickCount = 1;
		if(size > 1) {
			finalScore = W_POSITION_MULTIPLE*positionScore/((size-1)*BASE) 
						+ W_CLICK_MULTIPLE*clickcount/maxClickCount 
						+ W_PAGERANK_MULTIPLE*pageScore
						+ W_TFIDF*tfidf;
		}
		else {
			finalScore = W_CLICK*clickcount/maxClickCount
					    + W_PAGERANK*pageScore
					    + W_TF*wordtf[0];   
		}
	}
	
	public void secondScore(){
		finalScore += anchorScore;
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
	
	public String getUrl(){
		return url;
	}
	
	public String getTitle(){
		return title;
	}
	
	public List<String> analyzeURL(String url) throws IOException, InterruptedException {
		List<String> urlWords = new ArrayList<String>();
		if(url.startsWith("http://")){
			url = url.substring(7);
		}
		else if(url.startsWith("https://")){
			url = url.substring(8);
		}
		if(url.startsWith("www.")){
			url = url.substring(4);
		}
		url = ProcessUtils.stemContent(url);
		StringTokenizer tokenizer = new StringTokenizer(url, ProcessUtils.DELIMATOR);
		String word = "";
		while (tokenizer.hasMoreTokens()) {
			word = tokenizer.nextToken();
			if(word.equals("") || word.length()>20) continue;
			if(ProcessUtils.isNumber(word)) continue;
			if(!ProcessUtils.stopWords.contains(word)){
				urlWords.add(word);
			}
		}
		return urlWords;
	}
	
	public List<String> analyzeTitle(String content){
		List<String> titleWords = new ArrayList<String>();
		String store_text = ProcessUtils.stemContent(content);
		StringTokenizer tokenizer = new StringTokenizer(store_text, ProcessUtils.DELIMATOR);
		String word = "";
		while (tokenizer.hasMoreTokens()) {
			word = tokenizer.nextToken();
			if(word.equals("")) continue;
			boolean flag = false;
			for(int i=0;i<word.length();i++){
				if (Character.UnicodeBlock.of(word.charAt(i)) != Character.UnicodeBlock.BASIC_LATIN) {
					flag = true;
					break;
				}
			}	
			if(flag) continue;
			titleWords.add(word);
		}
		return titleWords;
	}
	
	public void analyzeURLTitle() throws Exception{
		DocURLTitle urltitle = DocURLTitle.load(id.array());
		url = urltitle.getURL();
		if(url == null) url = "";
		else url = url.toLowerCase();
		String title = urltitle.getTitle();
		this.title = title == null ? url:title;
		if(title == null) title = "";
		else title= title.toLowerCase();
		List<String> urlWords = analyzeURL(url);
		List<String> titleWords = analyzeTitle(title);
		List<String> urlcount = new ArrayList<String>();
		List<String> titlecount = new ArrayList<String>();
		for(int i=0;i<size;i++){
			String word = wordlist.get(i);
			if(urlWords.contains(word)) urlcount.add(word);
			if(titleWords.contains(wordlist.get(i))) titlecount.add(word);
		}
		anchorScore = (urlcount.size()*BASE_URL+titlecount.size()*BASE_TITLE)/(BASE_TITLE*size);
	}

	
}
