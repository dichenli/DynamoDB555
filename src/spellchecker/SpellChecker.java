package spellchecker;



// TODO: Auto-generated Javadoc
/**
 * The Class SpellChecker.
 */
public class SpellChecker {
	
	/** The dict. */
	Dictionary dict;
	
	/** The misspell. */
	FileCorrector misspell;
	
	/** The swap. */
	SwapCorrector swap;

	/**
	 * Instantiates a new spell checker.
	 */
	public SpellChecker(){
		dict = new Dictionary();
//		System.out.println("new dictionary");
		misspell = new FileCorrector();
//		System.out.println("new file corrector");
		swap = new SwapCorrector(dict);
//		System.out.println("new swap corrector");
	}
	
	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {
//		Dictionary dict = new Dictionary();
//		FileCorrector misspell = new FileCorrector();
//		SwapCorrector swap = new SwapCorrector(dict);
//		
////		misspell.writeToDB();
////		dict.writeToDB();
//		String test = "Zyzzogeton";
//		
//		String wrong = "embarrased";
//		System.out.println(misspell.getCorrection(wrong));
//		
////		System.out.println(swap.getCorrection(test));
////		System.out.println(dict.isWord(test));
		SpellChecker test = new SpellChecker();
		test.test();

	}
	
	/**
	 * Test.
	 */
	public void test(){
		Dictionary dict = new Dictionary();
		FileCorrector misspell = new FileCorrector();
		SwapCorrector swap = new SwapCorrector(dict);
		
//		misspell.writeToDB();
//		dict.writeToDB();
//		String test = "Zyzzogeton";
		
		String wrong = "abondon";
		System.out.println(misspell.getCorrection(wrong));
	}
	
	/**
	 * Checks if is word.
	 *
	 * @param word the word
	 * @return true, if is word
	 */
	public boolean isWord(String word){
		return dict.isWord(word);
	}
	
	/**
	 * Checks if is common misspell.
	 *
	 * @param word the word
	 * @return true, if is common misspell
	 */
	public boolean isCommonMisspell(String word){
		return misspell.containsMisspell(word);
	}
	
	/**
	 * get suggestion right word from 
	 * common misspelling file.
	 *
	 * @param wrong the wrong
	 * @return the right misspell
	 */
	public String getRightMisspell(String wrong){
		return misspell.getCorrection(wrong);
	}
	
	/**
	 * get suggestion right word from
	 * swapping neighbor characters.
	 *
	 * @param wrong the wrong
	 * @return the right swap
	 */
	public String getRightSwap(String wrong){
		return swap.getCorrection(wrong);
	}

}
