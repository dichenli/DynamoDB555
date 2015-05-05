package SearchUtils;

import java.util.List;

// TODO: Auto-generated Javadoc
/**
 * The Class FindURLThread.
 */
public class FindURLThread extends Thread{
	
	/** The index. */
	int index;
	
	/** The set. */
	List<DocResult> set;
	
	/** The size. */
	int size;
	
	/**
	 * Instantiates a new find url thread.
	 *
	 * @param index the index
	 * @param set the set
	 * @param size the size
	 */
	public FindURLThread(int index, List<DocResult> set, int size){
		this.index = index;
		this.set = set;
		this.size = size;
	}

	/* (non-Javadoc)
	 * @see java.lang.Thread#run()
	 */
	@Override
	public void run() {
		for(int i=0;i<size;i++){
			if(i%10 == index){
				DocResult doc = set.get(i);
				try {
					doc.analyzeURLTitle();
					doc.secondScore();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

}
