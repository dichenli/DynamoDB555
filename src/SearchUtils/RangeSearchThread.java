package SearchUtils;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import DynamoDB.InvertedIndex;

// TODO: Auto-generated Javadoc
/**
 * The Class RangeSearchThread.
 */
public class RangeSearchThread extends Thread{
	
	/** The threadindex. */
	int threadindex;
	
	/** The wordindex. */
	int wordindex;
	
	/** The word. */
	String word;
	
	/** The set. */
	HashMap<ByteBuffer, DocResult> set;
	
	/** The query info. */
	QueryInfo queryInfo;
	
	/**
	 * Instantiates a new range search thread.
	 *
	 * @param index the index
	 * @param wordindex the wordindex
	 * @param word the word
	 * @param set the set
	 * @param queryInfo the query info
	 */
	public RangeSearchThread(int index, int wordindex, String word, HashMap<ByteBuffer, DocResult> set, QueryInfo queryInfo){
		this.wordindex = wordindex;
		this.threadindex = index;
		this.word = word;
		this.set = set;
		this.queryInfo = queryInfo;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Thread#run()
	 */
	public void run(){
		System.out.println("get collection "+threadindex);
		List<InvertedIndex> collection = InvertedIndex.queryRange(word, threadindex);
		System.out.println("finish get collection "+threadindex);
		Iterator it = collection.iterator();
		while(it.hasNext()){
			InvertedIndex ii = (InvertedIndex)it.next();
//			System.out.println(count);
			ByteBuffer docID = ii.getId();
			double pageRank = ii.getPageRank();
			if(pageRank == -1) pageRank = 0;
			synchronized (set) {
				if (!set.containsKey(docID))
					set.put(docID, new DocResult(queryInfo, docID, pageRank));
				DocResult doc = set.get(docID);
				doc.setPositionList(wordindex, ii.PositionsSorted());
				doc.setTF(wordindex, ii.getTF());
			}
		}
		System.out.println(threadindex+"\tfinish");
	}

}
