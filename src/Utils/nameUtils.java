/**
 * 
 */
package Utils;

/**
 * @author dichenli
 *
 */
public class nameUtils {

	public static boolean isLetter(char c) {
		return (c >= 'a' && c <='z') || (c >='A' && c <= 'Z');
	}
	
	public static boolean isLetterOrUnderScore(char c) {
		return isLetter(c) || c == '_';
	}
	
	public static boolean isLetterOrHyphen(char c) {
		return isLetter(c) || c == '-';
	}
	
	public static boolean isLetterOrDigit(char c) {
		return isLetter(c) || isDigit(c);
	}
	
	public static boolean isLetterDigitOrHyphen(char c) {
		return isLetter(c) || isDigit(c) || c == '-';
	}
	
	public static boolean isDigit(char c) {
		return c >= '0' && c <='9';
	}
	
	public static boolean isHyphen(char c) {
		return c == '-';
	}
	
	public static boolean isPeriod(char c) {
		return c == '.';
	}
	
	public static boolean isValidNameCharacter(char c) {
		return isLetterOrUnderScore(c) || isHyphen(c) || isDigit(c) || isPeriod(c);
	}
	
	public static boolean isNullOrEmpty(String s) {
		return s == null || s.equals("");
	}
	
	public static boolean isValidName(String name) {
		/* From http://www.w3schools.com/xml/xml_elements.asp
		 * Element names are case-sensitive
		 * Element names must start with a letter or underscore
		 * Element names cannot start with the letters xml (or XML, or Xml, etc)
		 * Element names can contain letters, digits, hyphens, underscores, and periods
		 * Element names cannot contain spaces
		 */
		name = name.trim();
		if(isNullOrEmpty(name)) {
			return false;
		}
		if(!isLetterOrUnderScore(name.charAt(0))) {
			return false;
		}
		for(int i = 1; i < name.length(); i++) {
			if(!isValidNameCharacter(name.charAt(i))) {
				return false;
			}
		}
		return true;
	}
}
