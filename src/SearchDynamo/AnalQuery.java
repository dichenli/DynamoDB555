package SearchDynamo;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import DynamoDB.DocURL;
import DynamoDB.InvertedIndex;
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
				if (!set.containsKey(docID))
					set.put(docID, new DocResult(docID, size, queryInfo.getWindowlist(), idflist));
				set.get(docID).setPositionList(i, ii.PositionsSorted());
				if(ii.getType() == 0 ) {
					set.get(docID).setTF(i, ii.getTF());
				}
				else {
					set.get(docID).setAnchor(i, ii.getType());
				}
			}
		}
		List<DocResult> intersection = new ArrayList<DocResult>();
		for (ByteBuffer docID : set.keySet()) {
			if (set.get(docID).containsAll()) {
				intersection.add(set.get(docID));
			}
		}
		
		// compute rank each doc
		for (DocResult doc : intersection) {
			doc.calculateScore();
		}
		Collections.sort(intersection, new Comparator<DocResult>() {
	        @Override
	        public int compare(DocResult o1, DocResult o2) {
	            return o2.compareTo(o1);
	        }
	    });
		
		List<SearchResult> responses = new ArrayList<SearchResult>();
		int responsesize = Math.min(responses.size(), 20);
		for(int i=0;i<responsesize;i++){
			DocResult doc = intersection.get(i);
			String url = DocURL.load(doc.getDocID().array()).getURL();
			SearchResult sr = new SearchResult(url);
			responses.add(sr);
//			System.out.println(DocURL.load(doc.getDocID().array()).getURL() +"\t"+doc.getFinalScore());
//			for(List<Integer> w:doc.getPositions()){
//				System.out.println(w);
//			}
		}
		return responses;

	}

}
