package SearchUtils;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import DynamoDB.InvertedIndex;

public class FindWordThread extends Thread{
	
	int wordindex;
	String word;
	HashMap<ByteBuffer, DocResult> set;
	QueryInfo queryInfo;
	
	public FindWordThread(int i, String word, HashMap<ByteBuffer, DocResult> set, QueryInfo queryInfo){
		this.wordindex = i;
		this.word = word;
		this.set = set;
		this.queryInfo = queryInfo;
	}
	
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
