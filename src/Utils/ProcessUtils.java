package Utils;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashSet;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import snowballstemmer.PorterStemmer;

// TODO: Auto-generated Javadoc
/**
 * The Class ProcessUtils.
 */
public class ProcessUtils {
	
	/** The Constant DELIMATOR. */
	public static final String DELIMATOR = " \t\n\r\"'-_/.,:;|{}[]!@#%^&*()<>=+`~?";
	
	/** The stop words. */
	public static HashSet<String> stopWords = new HashSet<String>();

	static {
		String[] lists = { "edu", "com", "html", "htm", "xml", "php", "org",
				"gov", "net", "int", "jpg", "png", "bmp", "jpeg", "pdf", "asp",
				"aspx" };
		for (String word : lists) {
			stopWords.add(word);
		}
	}
	
	/**
	 * Checks if is number.
	 *
	 * @param s the s
	 * @return true, if is number
	 */
	public static boolean isNumber(String s) {
		String pattern = "\\d+";
		// Create a Pattern object
		Pattern r = Pattern.compile(pattern);
		// Now create matcher object.
		Matcher m = r.matcher(s);
		return m.find();
	}
	
	/**
	 * Stem content.
	 *
	 * @param content the content
	 * @return the string
	 */
	public static String stemContent(String content) {
		StringTokenizer tokenizer = new StringTokenizer(content, DELIMATOR);
		String word = "";
		PorterStemmer stemmer = new PorterStemmer();
		StringBuilder sb = new StringBuilder();
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
			stemmer.setCurrent(word);
			if(stemmer.stem()){
				sb.append(stemmer.getCurrent());
				sb.append(" ");
			}
		}
		return new String(sb);
	}
	
	/**
	 * To big integer.
	 *
	 * @param key the key
	 * @return the string
	 */
	public static String toBigInteger(String key) {
		try {
			MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
			messageDigest.update(key.getBytes());
			byte[] bytes = messageDigest.digest();
			Formatter formatter = new Formatter();
			for (int i = 0; i < bytes.length; i++) {
				formatter.format("%02x", bytes[i]);
			}
			String resString = formatter.toString();
			formatter.close();
			return String.valueOf(new BigInteger(resString, 16));
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return String.valueOf(new BigInteger("0", 16));
	}
	
	/**
	 * Analyze url.
	 *
	 * @param url the url
	 * @return the list
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws InterruptedException the interrupted exception
	 */
	public static List<String> analyzeURL(String url) throws IOException, InterruptedException {
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
				System.out.println(word);
			}
		}
		return urlWords;
	}
	
	/**
	 * Analyze title.
	 *
	 * @param content the content
	 * @return the list
	 */
	public static List<String> analyzeTitle(String content){
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
	
	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws InterruptedException the interrupted exception
	 */
	public static void main(String[] args) throws IOException, InterruptedException{
		String url = "http://www.dmoz.org/Computers/Computer_Science/";
		analyzeURL(url);
	}

}


