package SearchDynamo;

import java.io.BufferedReader;
import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import S3.S3FileReader;
import snowballstemmer.PorterStemmer;

/**
 * @author dichenli
 * given a DocID, find the document content, then find the string that match the
 * given positions. Reverse Engineer of IndexerMapper code
 */
public class WordHeat {

	public static String findPosition(String rawContent, String[] words) {
		//remove all tags, keep only words
		String content = html2text(rawContent);
		
		int prefix = 50;
		int suffix = 50;
		String begin = "(.{0," + prefix + "})";
		String end = "(.{0," + suffix + "})";
		String regex = begin;
		PorterStemmer stemmer = new PorterStemmer();
		//compose regex, example: /.{0,50}\b(Regular.*?)\b.+?\b(express.*?)\b.{0,50}/
		for (int i = 0; i < words.length; i++) {
			if(words[i].equals("")) continue;
			stemmer.setCurrent(words[i]);
			if(stemmer.stem()){
				String word = stemmer.getCurrent();				
				regex += ("\\b(" + Pattern.quote(word) + ".*?)\\b");
				if(i < words.length - 1) {
					regex += "(.+?)";
				}
			}
		}
		regex += end;
		System.out.println(regex);
		//try to match
		Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(content);
		if(!matcher.find()) {
			return null;
		}
//		int min = Integer.MAX_VALUE;
//		try { //find the shortest match
//			for(int i = 0; i < 3; i++) {
//				matcher.find(i);
//				String matched = matcher.group(0);
//				System.out.println(matched + "\t" + matched.length());
//				if(matched.length() < min) {
//					min = i;
//				}
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//			min = 0;
//		}
		int min = 0; //no more things above, to save time
		
		//compose the string to be presented
//		matcher.find(min);
		String result = matcher.group(1);
		if(result.length() >= prefix) { //if the match is the beginning of the file
			result = "..." + result;
		}
		int index = 2;
		for(int i = 0; i < words.length - 1; i++) {
			result += ("<b>" + matcher.group(index) + "</b>");
			index++;
			String inner = matcher.group(index); 
			index++;
			if(inner.length() > 100) { //if two words are too far away, ignore some intervening words 
				inner = inner.substring(0, 15) + "..." + inner.substring(inner.length() - 15);
			}
			result += inner;
		}
		result += ("<b>" + matcher.group(index) + "</b>"); //add the last word
		String endMatch = matcher.group(index + 1); //add the end
		if(endMatch.length() >= suffix) { //if there is still text after this match
			endMatch += "...";
		}
		result += endMatch;
		return result;
	}


	public static String html2text(String content) {
		return Jsoup.parse(content).text();
	}

	public static void main(String[] args) {
		String[] words = {"regular", "expressions"};
		System.out.println(findPosition("Regular expressions are an incredibly powerful tool, "
				+ "but can be rather tricky to get exactly right. This is a "
				+ "website that I wrote so I could quickly and easily test "
				+ "regular shfwalfbalfbliraef  efbwael lwFBLAEWF AW WE;FsdfasdfsadfsdfasdfsdafsadfsadfsdfdsafB WLAKFSBF AWELIekr bgldfgv ag erg  expressions during development.", 
				words));
	}

}
