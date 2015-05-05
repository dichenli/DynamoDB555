package SearchUtils;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import DynamoDB.InvertedIndex;

public class RangeSearchThread extends Thread{
	
	int threadindex;
	int wordindex;
	String word;
	HashMap<ByteBuffer, DocResult> set;
	QueryInfo queryInfo;
	
	public RangeSearchThread(int index, int wordindex, String word, HashMap<ByteBuffer, DocResult> set, QueryInfo queryInfo){
		this.wordindex = wordindex;
		this.threadindex = index;
		this.word = word;
		this.set = set;
		this.queryInfo = queryInfo;
	}
	
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
