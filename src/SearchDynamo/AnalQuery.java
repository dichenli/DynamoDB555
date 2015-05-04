package SearchDynamo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import DynamoDB.DocURLTitle;
import DynamoDB.InvertedIndex;
import DynamoDB.QueryRecord;
import SearchUtils.DocResult;
import SearchUtils.QueryInfo;
import SearchUtils.SearchResult;
import Utils.ProcessUtils;

import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedQueryList;


public class AnalQuery {
	
	public static int setClickScore(String query, HashMap<ByteBuffer, DocResult> set){
		int maxcount = 0;
		PaginatedQueryList<QueryRecord> countlist = QueryRecord.find(query);
		for(QueryRecord qr:countlist){
			int count = qr.getCount();
			if(count > maxcount) maxcount = count;
			ByteBuffer id = qr.getId();
			set.get(id).setClickScore(count);
		}
		return maxcount;
	}
	

	public static List<DocResult> search(String query) throws Exception {
		QueryInfo queryInfo = new QueryInfo(query);
		
		// remove words with low idf
		List<String> wordlist = queryInfo.getWordlist();
		List<Double> idflist = queryInfo.getIDFlist();
		int size = wordlist.size();
		HashMap<ByteBuffer, DocResult> set = new HashMap<ByteBuffer, DocResult>();
		for (int i = 0; i < size; i++) {
			String word = wordlist.get(i);
			System.out.println(word);
			List<InvertedIndex> collection = InvertedIndex.query(word);
			int count = 0;
			Iterator it = collection.iterator();
			while(it.hasNext()){
				InvertedIndex ii = (InvertedIndex)it.next();
				count++;
//				System.out.println(count);
				ByteBuffer docID = ii.getId();
				double pageRank = ii.getPageRank();
				if(pageRank == -1) pageRank = 0;		
				if (!set.containsKey(docID))
					set.put(docID, new DocResult(wordlist, query, docID, pageRank, size, queryInfo.getWindowlist(), idflist));
				DocResult doc = set.get(docID);
				doc.setPositionList(i, ii.PositionsSorted());
				doc.setTF(i, ii.getTF());
//				else {
//					System.out.println("get type");
//					doc.setAnchor(i, ii.getType());
//				}
			}
		}
		System.out.println("finish get word");
		List<DocResult> intersection = new ArrayList<DocResult>();
		for (ByteBuffer docID : set.keySet()) {
			if (set.get(docID).containsAll()) {
				intersection.add(set.get(docID));
			}
		}
		
		// minimize the page source set
		List<DocResult> minimizedSet = new ArrayList<DocResult>();
		if(size == 1) minimizedSet = intersection;
		else{
			for (DocResult doc : intersection) {
				int positionScore = doc.setPositionScore();
				if(positionScore > 0) minimizedSet.add(doc);
			}
		}
		System.out.println("Minimized Set "+minimizedSet.size());
//		int maxClickCount = setClickScore(query, set);
		if(minimizedSet.size()<100 && size != 1){
			minimizedSet = intersection;
		}
		
		// first score (including position check, page rank, tfidf)
		for (DocResult doc : minimizedSet){
			doc.firstScore(1);
		}
		
		Collections.sort(minimizedSet, new Comparator<DocResult>() {
	        @Override
	        public int compare(DocResult o1, DocResult o2) {
	            return o2.compareTo(o1);
	        }
	    });
		
		minimizedSet = minimizedSet.subList(0, Math.min(minimizedSet.size(), 100));
		
		// second score (including url and title check)
		for (int i=0;i<minimizedSet.size();i++){
			DocResult doc = minimizedSet.get(i);
			doc.analyzeURLTitle();
			doc.secondScore();
		}
		
		Collections.sort(minimizedSet, new Comparator<DocResult>() {
	        @Override
	        public int compare(DocResult o1, DocResult o2) {
	            return o2.compareTo(o1);
	        }
	    });
		
		return minimizedSet.subList(0, Math.min(minimizedSet.size(), 10));
//		int responsesize = Math.min(minimizedSet.size(), 20);
//		List<DocResult> responses = new ArrayList<DocResult>();
//		for(int i=0;i<responsesize;i++){
//			DocResult doc = minimizedSet.get(i);
//			byte[] docID = doc.getDocID().array();
//			responses.add(doc);
//			System.out.println(url +"\t"+doc.getAnchorScore()+"\t"+doc.getPositionScore()+"\t"+doc.getPageRank()+"\t"+doc.getFinalScore());
//			for(List<Integer> w:doc.getPositions()){
//				System.out.println(w);
//			}
//		}
//		return responses;

	}
	

	public static void main(String[] args) throws Exception{
//		List<SearchResult> response = search("computer science");
//		for(SearchResult sr:response){
//			System.out.println(sr.getUrl());
//		}
	}

}
