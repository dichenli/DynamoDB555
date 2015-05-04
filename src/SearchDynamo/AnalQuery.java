package SearchDynamo;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedQueryList;

import DynamoDB.DocURL;
import DynamoDB.DocURLTitle;
import DynamoDB.InvertedIndex;
import DynamoDB.QueryRecord;
import SearchUtils.DocResult;
import SearchUtils.QueryInfo;
import SearchUtils.SearchResult;


public class AnalQuery {
	
	public static void main(String[] args) throws Exception{
		List<SearchResult> response = search("computer science");
		for(SearchResult sr:response){
			System.out.println(sr.getUrl());
		}
	}
	
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

	public static List<SearchResult> search(String query) throws Exception {
		QueryInfo queryInfo = new QueryInfo(query);
		
		// remove words with low idf
		List<String> wordlist = queryInfo.getWordlist();
		List<Double> idflist = queryInfo.getIDFlist();
		int size = wordlist.size();
		HashMap<ByteBuffer, DocResult> set = new HashMap<ByteBuffer, DocResult>();
		for (int i = 0; i < size; i++) {
			String word = wordlist.get(i);
//			System.out.println(word);
			List<InvertedIndex> collection = InvertedIndex.query(word);
			for (InvertedIndex ii : collection) {
				ByteBuffer docID = ii.getId();
				if(ii.getType() == 0){			
					if (!set.containsKey(docID))
						set.put(docID, new DocResult(query, docID, size, queryInfo.getWindowlist(), idflist));
					set.get(docID).setPositionList(i, ii.PositionsSorted());
				}
				else {
					if(set.containsKey(docID)){
						set.get(docID).setAnchor(i, ii.getType());
					}
				}
			}
		}
		List<DocResult> intersection = new ArrayList<DocResult>();
		for (ByteBuffer docID : set.keySet()) {
			if (set.get(docID).containsAll()) {
				intersection.add(set.get(docID));
			}
		}
		
		// minimize the page source set
		List<DocResult> minimizedSet = new ArrayList<DocResult>();
		for (DocResult doc : intersection) {
			int positionScore = doc.setPositionScore();
			if(positionScore > 0) minimizedSet.add(doc);
		}
		System.out.println("Minimized Set "+minimizedSet.size());
		
		int maxClickCount = setClickScore(query, set);
		if(minimizedSet.size()<10){
			minimizedSet = intersection;
		}
		
		// compute the score
		for (DocResult doc : minimizedSet){
			doc.calculateScore(maxClickCount);
		}
		
		Collections.sort(minimizedSet, new Comparator<DocResult>() {
	        @Override
	        public int compare(DocResult o1, DocResult o2) {
	            return o2.compareTo(o1);
	        }
	    });
		
		List<SearchResult> responses = new ArrayList<SearchResult>();
		int responsesize = Math.min(intersection.size(), 20);
		for(int i=0;i<responsesize;i++){
			DocResult doc = minimizedSet.get(i);
			byte[] docID = doc.getDocID().array();
			DocURLTitle docURLTitle = DocURLTitle.load(docID); //get url and title from the new function
			String url = docURLTitle.getURL();
			String title = docURLTitle.getTitle();
			SearchResult sr = new SearchResult(url, docID, title);
			responses.add(sr);
			System.out.println(DocURL.load(doc.getDocID().array()).getURL() +"\t"+doc.getPositionScore()+"\t"+doc.getClickCount()+"\t"+doc.getFinalScore());
//			for(List<Integer> w:doc.getPositions()){
//				System.out.println(w);
//			}
		}
		return responses;

	}

}
