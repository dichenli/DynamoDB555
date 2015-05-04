package Utils;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import snowballstemmer.PorterStemmer;

public class ProcessUtils {
	
	public static final String DELIMATOR = " \t\n\r\"'-_/.,:;|{}[]!@#%^&*()<>=+`~?";
	
	public static HashSet<String> stopWords = new HashSet<String>();

	static {
		String[] lists = { "edu", "com", "html", "htm", "xml", "php", "org",
				"gov", "net", "int", "jpg", "png", "bmp", "jpeg", "pdf", "asp",
				"aspx" };
		for (String word : lists) {
			stopWords.add(word);
		}
	}
	
	public static boolean isNumber(String s) {
		String pattern = "\\d+";
		// Create a Pattern object
		Pattern r = Pattern.compile(pattern);
		// Now create matcher object.
		Matcher m = r.matcher(s);
		return m.find();
	}
	
	public static String stemContent(String content) {
		StringTokenizer tokenizer = new StringTokenizer(content, DELIMATOR);
		String word = "";
		PorterStemmer stemmer = new PorterStemmer();
		StringBuilder sb = new StringBuilder();
		while (tokenizer.hasMoreTokens()) {
			word = tokenizer.nextToken();
			System.out.println(word); 
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

}
