package SearchUtils;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import DynamoDB.InvertedIndex;

public class FindWordThread extends Thread{
	
	int index;
	String word;
	HashMap<ByteBuffer, DocResult> set;
	QueryInfo queryInfo;
	
	public FindWordThread(int i, String word, HashMap<ByteBuffer, DocResult> set, QueryInfo queryInfo){
		this.index = i;
		this.word = word;
		this.set = set;
		this.queryInfo = queryInfo;
	}
	
	public void run() {
		List<InvertedIndex> collection = InvertedIndex.query(word);
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
				doc.setPositionList(index, ii.PositionsSorted());
				doc.setTF(index, ii.getTF());
			}
		}
		
	}


}
