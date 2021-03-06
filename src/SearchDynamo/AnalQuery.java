package SearchDynamo;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import DynamoDB.InvertedIndex;
import DynamoDB.QueryRecord;
import SearchUtils.DocResult;
import SearchUtils.FindURLThread;
import SearchUtils.FindWordThread;
import SearchUtils.QueryInfo;

import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedQueryList;


// TODO: Auto-generated Javadoc
/**
 * The Class AnalQuery.
 */
public class AnalQuery {
	
	/**
	 * Sets the click score.
	 *
	 * @param query the query
	 * @param set the set
	 * @return the int
	 */
	public static int setClickScore(String query, HashMap<ByteBuffer, DocResult> set){
		int maxcount = 0;
		System.out.println("set click score");
		PaginatedQueryList<QueryRecord> countlist = QueryRecord.find(query);
		System.out.println(countlist.size());
		for(QueryRecord qr:countlist){
			int count = qr.getCount();
			if(count > maxcount) maxcount = count;
			ByteBuffer id = qr.getId();
			if(set.get(id) != null) set.get(id).setClickScore(count);
		}
		return maxcount;
	}
	

	/**
	 * Search.
	 *
	 * @param query the query
	 * @return the list
	 * @throws Exception the exception
	 */
	public static List<DocResult> search(String query) throws Exception {
		QueryInfo queryInfo = null;
		try {
			queryInfo = new QueryInfo(query);
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Result not found!");
			throw e;
		}
		
		// remove words with low idf
		List<String> wordlist = queryInfo.getWordlist();
		List<Double> idflist = queryInfo.getIDFlist();
		int size = wordlist.size();
		if(size == 0) return new ArrayList<DocResult>();
		
		HashMap<ByteBuffer, DocResult> set = new HashMap<ByteBuffer, DocResult>();
		Thread[] findWordThreads = new FindWordThread[size];
		for(int i=0;i<size;i++){
			findWordThreads[i] = new FindWordThread(i, wordlist.get(i), set, queryInfo);
			findWordThreads[i].start();
		}
		for(int i=0;i<size;i++){
			findWordThreads[i].join();
		}
		
//		for (int i = 0; i < size; i++) {
//			String word = wordlist.get(i);
//			System.out.println(word);
//			List<InvertedIndex> collection = InvertedIndex.query(word);
//			Iterator it = collection.iterator();
//			while(it.hasNext()){
//				InvertedIndex ii = (InvertedIndex)it.next();
////				System.out.println(count);
//				ByteBuffer docID = ii.getId();
//				double pageRank = ii.getPageRank();
//				if(pageRank == -1) pageRank = 0;		
//				if (!set.containsKey(docID))
//					set.put(docID, new DocResult(queryInfo, docID, pageRank));
//				DocResult doc = set.get(docID);
//				doc.setPositionList(i, ii.PositionsSorted());
//				doc.setTF(i, ii.getTF());
//			}
//		}
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
				if(!doc.containsAll()) System.out.println("doc does not contains all");
				int positionScore = doc.setPositionScore();
				if(positionScore > 0) minimizedSet.add(doc);
			}
		}
		System.out.println("Minimized Set "+minimizedSet.size());
		int maxClickCount = setClickScore(query, set);
		System.out.println(maxClickCount);
		if(minimizedSet.size()<100 && size != 1){
			minimizedSet = intersection;
		}
		
		// first score (including position check, page rank, tfidf)
		for (DocResult doc : minimizedSet){
			doc.firstScore(maxClickCount);
		}
		
		Collections.sort(minimizedSet, new Comparator<DocResult>() {
	        @Override
	        public int compare(DocResult o1, DocResult o2) {
	            return o2.compareTo(o1);
	        }
	    });
		
		int setsize = minimizedSet.size();
		minimizedSet = minimizedSet.subList(0, Math.min(setsize, 100));
		
		System.out.println("before Thread start: "+minimizedSet.size());
		Thread[] urlThreads = new FindURLThread[10];
		int finalsize = minimizedSet.size();
		for(int i=0;i<10;i++){
			urlThreads[i] = new FindURLThread(i, minimizedSet, finalsize);
			urlThreads[i].start();
		}
		for(int i=0;i<10;i++){
			urlThreads[i].join();
		}
		// second score (including url and title check)
//		for (int i=0;i<minimizedSet.size();i++){
//			DocResult doc = minimizedSet.get(i);
//			doc.analyzeURLTitle();
//			doc.secondScore();
//		}
		
		//remove duplicates by url comparison
		minimizedSet = removeDupURL(minimizedSet);
		
		Collections.sort(minimizedSet, new Comparator<DocResult>() {
	        @Override
	        public int compare(DocResult o1, DocResult o2) {
	            return o2.compareTo(o1);
	        }
	    });
		
		return minimizedSet.subList(0, Math.min(minimizedSet.size(), 100));
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
	

	private static List<DocResult> removeDupURL(List<DocResult> minimizedSet) {
//		Collections.sort(minimizedSet, new Comparator<DocResult>() {
//			@Override
//			public int compare(DocResult o1, DocResult o2) {
//				return o1.url.compareTo(o2.url);
//			}
//		});
		HashMap<String, DocResult> set = new HashMap<String, DocResult>();
		for(DocResult item : minimizedSet) {
			String key = item.title;
			if(key == null) {
				System.out.println("======null url=====");
				continue;
			}
//			if(key.charAt(key.length() - 1) != '/') {
//				key += '/';
//			}
			set.put(key, item);
		}
		return new ArrayList<DocResult>(set.values());
	}


	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws Exception the exception
	 */
	public static void main(String[] args) throws Exception{
//		List<SearchResult> response = search("computer science");
//		for(SearchResult sr:response){
//			System.out.println(sr.getUrl());
//		}
	}

}
