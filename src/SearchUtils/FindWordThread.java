package SearchUtils;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import DynamoDB.InvertedIndex;

// TODO: Auto-generated Javadoc
/**
 * The Class FindWordThread.
 */
public class FindWordThread extends Thread{
	
	/** The wordindex. */
	int wordindex;
	
	/** The word. */
	String word;
	
	/** The set. */
	HashMap<ByteBuffer, DocResult> set;
	
	/** The query info. */
	QueryInfo queryInfo;
	
	/**
	 * Instantiates a new find word thread.
	 *
	 * @param i the i
	 * @param word the word
	 * @param set the set
	 * @param queryInfo the query info
	 */
	public FindWordThread(int i, String word, HashMap<ByteBuffer, DocResult> set, QueryInfo queryInfo){
		this.wordindex = i;
		this.word = word;
		this.set = set;
		this.queryInfo = queryInfo;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Thread#run()
	 */
	public void run() {
		Thread[] rangeThreads = new RangeSearchThread[16];
		for(int i=0;i<16;i++){
			rangeThreads[i] = new RangeSearchThread(i, wordindex, word, set, queryInfo);
			rangeThreads[i].start();
		}
		for(int i=0;i<16;i++){
			try {
				rangeThreads[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}


}
