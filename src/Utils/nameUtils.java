/**
 * 
 */
package Utils;

// TODO: Auto-generated Javadoc
/**
 * The Class nameUtils.
 *
 * @author dichenli
 */
public class nameUtils {

	/**
	 * Checks if is letter.
	 *
	 * @param c the c
	 * @return true, if is letter
	 */
	public static boolean isLetter(char c) {
		return (c >= 'a' && c <='z') || (c >='A' && c <= 'Z');
	}
	
	/**
	 * Checks if is letter or under score.
	 *
	 * @param c the c
	 * @return true, if is letter or under score
	 */
	public static boolean isLetterOrUnderScore(char c) {
		return isLetter(c) || c == '_';
	}
	
	/**
	 * Checks if is letter or hyphen.
	 *
	 * @param c the c
	 * @return true, if is letter or hyphen
	 */
	public static boolean isLetterOrHyphen(char c) {
		return isLetter(c) || c == '-';
	}
	
	/**
	 * Checks if is letter or digit.
	 *
	 * @param c the c
	 * @return true, if is letter or digit
	 */
	public static boolean isLetterOrDigit(char c) {
		return isLetter(c) || isDigit(c);
	}
	
	/**
	 * Checks if is letter digit or hyphen.
	 *
	 * @param c the c
	 * @return true, if is letter digit or hyphen
	 */
	public static boolean isLetterDigitOrHyphen(char c) {
		return isLetter(c) || isDigit(c) || c == '-';
	}
	
	/**
	 * Checks if is digit.
	 *
	 * @param c the c
	 * @return true, if is digit
	 */
	public static boolean isDigit(char c) {
		return c >= '0' && c <='9';
	}
	
	/**
	 * Checks if is hyphen.
	 *
	 * @param c the c
	 * @return true, if is hyphen
	 */
	public static boolean isHyphen(char c) {
		return c == '-';
	}
	
	/**
	 * Checks if is period.
	 *
	 * @param c the c
	 * @return true, if is period
	 */
	public static boolean isPeriod(char c) {
		return c == '.';
	}
	
	/**
	 * Checks if is valid name character.
	 *
	 * @param c the c
	 * @return true, if is valid name character
	 */
	public static boolean isValidNameCharacter(char c) {
		return isLetterOrUnderScore(c) || isHyphen(c) || isDigit(c) || isPeriod(c);
	}
	
	/**
	 * Checks if is null or empty.
	 *
	 * @param s the s
	 * @return true, if is null or empty
	 */
	public static boolean isNullOrEmpty(String s) {
		return s == null || s.equals("");
	}
	
	/**
	 * Checks if is valid name.
	 *
	 * @param name the name
	 * @return true, if is valid name
	 */
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
