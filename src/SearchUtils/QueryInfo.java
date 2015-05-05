package SearchUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import snowballstemmer.PorterStemmer;
import DynamoDB.IDF;

// TODO: Auto-generated Javadoc
/**
 * The Class QueryInfo.
 */
public class QueryInfo {

	/** The Constant PARSER. */
	public static final String PARSER = " \t\n\r\"'-_/.,:;|{}[]!@#%^&*()<>=+`~?";
	
	/** The Constant LIMIT. */
	private static final double LIMIT = 1;
	
	/** The Constant WINDOW. */
	private static final int WINDOW = 3;
	
	/** The wordlist. */
	List<String> wordlist = new ArrayList<String>();
	
	/** The indexlist. */
	List<Integer> indexlist = new ArrayList<Integer>();
	
	/** The idflist. */
	List<Double> idflist = new ArrayList<Double>();
	
	/** The windowlist. */
	int[] windowlist;
	
	/**
	 * Instantiates a new query info.
	 *
	 * @param query the query
	 */
	public QueryInfo(String query){
		List<String> parseQuery = stemContent(query.toLowerCase());
		List<String> finalWords = new ArrayList<String>();
		List<Integer> finalIndexs = new ArrayList<Integer>();
		List<Double> finalidfs = new ArrayList<Double>();
		for(int i=0;i<parseQuery.size();i++){
			String word = parseQuery.get(i);
			System.out.println(word);
			finalWords.add(word);
			finalIndexs.add(i);
			IDF idfResult;
			double idf = 1;
			try {
				idfResult = IDF.load(word);
				idf = idfResult.getidf();
			} catch (Exception e) {
				e.printStackTrace();
				idf = 1;
			}
			finalidfs.add(idf);
			System.out.println(idf);
			if(idf > LIMIT){
				wordlist.add(word);
				indexlist.add(i);
				idflist.add(idf);
			}
		}
		if(wordlist.size() == 0){
			wordlist = finalWords;
			indexlist = finalIndexs;
			idflist = finalidfs;
		}
		windowlist = new int[indexlist.size()-1];
		for(int i=0;i<indexlist.size()-1;i++){
			windowlist[i] = -1+indexlist.get(i+1)-indexlist.get(i)+WINDOW;
		}
	}
	
	/**
	 * Gets the wordlist.
	 *
	 * @return the wordlist
	 */
	public List<String> getWordlist(){
		return wordlist;
	}
	
	/**
	 * Gets the indexlist.
	 *
	 * @return the indexlist
	 */
	public List<Integer> getIndexlist(){
		return indexlist;
	}
	
	/**
	 * Gets the ID flist.
	 *
	 * @return the ID flist
	 */
	public List<Double> getIDFlist(){
		return idflist;
	}
	
	/**
	 * Gets the windowlist.
	 *
	 * @return the windowlist
	 */
	public int[] getWindowlist(){
		return windowlist;
	}
	
	
	
	/**
	 * Gets the size.
	 *
	 * @return the size
	 */
	public int getSize(){
		return wordlist.size();
	}
	
	/**
	 * Stem content.
	 *
	 * @param content the content
	 * @return the list
	 */
	public static List<String> stemContent(String content) {
		StringTokenizer tokenizer = new StringTokenizer(content, PARSER);
		String word = "";
		PorterStemmer stemmer = new PorterStemmer();
		List<String> parseQuery = new ArrayList<String>();
		while (tokenizer.hasMoreTokens()) {
			word = tokenizer.nextToken();
			if (word.equals(""))
				continue;
			boolean flag = false;
			for (int i = 0; i < word.length(); i++) {
				if (Character.UnicodeBlock.of(word.charAt(i)) != Character.UnicodeBlock.BASIC_LATIN) {
					flag = true;
					break;
				}
			}
			if (flag)
				continue;
			int i = 0;
			while (i < word.length()
					&& (!Character.isLetter(word.charAt(i)) && !Character
							.isDigit(word.charAt(i)))) {
				i++;
			}
			if (i >= word.length())
				continue;
			word = word.substring(i);
			i = word.length() - 1;
			while (i >= 0
					&& (!Character.isLetter(word.charAt(i)) && !Character
							.isDigit(word.charAt(i)))) {
				i--;
			}
			if (i < 0)
				continue;
			word = word.substring(0, i + 1);
			stemmer.setCurrent(word);
			if (stemmer.stem()) {
				parseQuery.add(stemmer.getCurrent());
			}
		}
		return parseQuery;
	}
}
