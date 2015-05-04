package spellchecker;



public class SpellChecker {
	Dictionary dict;
	FileCorrector misspell;
	SwapCorrector swap;

	public SpellChecker(){
		dict = new Dictionary();
//		System.out.println("new dictionary");
		misspell = new FileCorrector();
//		System.out.println("new file corrector");
		swap = new SwapCorrector(dict);
//		System.out.println("new swap corrector");
	}
	
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
	
	public boolean isWord(String word){
		return dict.isWord(word);
	}
	
	public boolean isCommonMisspell(String word){
		return misspell.containsMisspell(word);
	}
	
	/**
	 * get suggestion right word from 
	 * common misspelling file
	 * */
	public String getRightMisspell(String wrong){
		return misspell.getCorrection(wrong);
	}
	
	/**
	 * get suggestion right word from
	 * swapping neighbor characters
	 * */
	public String getRightSwap(String wrong){
		return swap.getCorrection(wrong);
	}

}
