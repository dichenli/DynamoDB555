package SearchUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import snowballstemmer.PorterStemmer;
import DynamoDB.IDF;

public class QueryInfo {

	public static final String PARSER = " \t\n\r\"'-_/.,:;|{}[]!@#%^&*()<>=+`~?";
	private static final double LIMIT = 1;
	private static final int WINDOW = 3;
	
	List<String> wordlist = new ArrayList<String>();
	List<Integer> indexlist = new ArrayList<Integer>();
	List<Double> idflist = new ArrayList<Double>();
	int[] windowlist;
	
	public QueryInfo(String query) throws Exception{
		List<String> parseQuery = stemContent(query.toLowerCase());
		for(int i=0;i<parseQuery.size();i++){
			String word = parseQuery.get(i);
			double idf = IDF.load(word).getidf();
			System.out.println(idf);
			if(idf > LIMIT){
				wordlist.add(word);
				indexlist.add(i);
				idflist.add(idf);
			}
		}
		windowlist = new int[indexlist.size()-1];
		for(int i=0;i<indexlist.size()-1;i++){
			windowlist[i] = -1+indexlist.get(i+1)-indexlist.get(i)+WINDOW;
		}
	}
	
	public List<String> getWordlist(){
		return wordlist;
	}
	
	public List<Integer> getIndexlist(){
		return indexlist;
	}
	
	public List<Double> getIDFlist(){
		return idflist;
	}
	
	public int[] getWindowlist(){
		return windowlist;
	}
	
	
	
	public int getSize(){
		return wordlist.size();
	}
	
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
