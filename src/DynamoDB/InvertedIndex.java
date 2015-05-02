/**
 * 
 */
package DynamoDB;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import S3.S3FileReader;
import S3.S3Iterator;
import Utils.BinaryUtils;
import Utils.IOUtils;
import Utils.TimeUtils;

import com.amazonaws.services.dynamodbv2.datamodeling.*;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.sun.xml.internal.fastinfoset.algorithm.BuiltInEncodingAlgorithm.WordListener;
/**
 * @author dichenli
 *
 */
@DynamoDBTable(tableName="InvertedIndex2")
public class InvertedIndex {
	
	static String tableName = "InvertedIndex2"; //need to sync with @DynamoDBTable(tableName="xx")
	static String hashKey = "word";
	static String rangeKey = "id";
	static long readCapacity = 1L;
	static long writeCapacity = 1000L;
	
	byte[] id; //binary data, docID
	String word; 
	HashSet<Integer> positions; //position of the word in document
	double tf; //TF value
	Double idf;
	Double pagerank;
	int type;


	public InvertedIndex(String word2, byte[] id2, double tf2,
			HashSet<Integer> positions2/*, int type*/) {
		this.word = word2;
		this.id = id2;
		this.positions = positions2;
		this.tf = tf2;
		this.type = -1;//type;
		this.idf = (double)-1;
		this.pagerank = (double)-1;
	}

	@DynamoDBRangeKey(attributeName="id")
	public ByteBuffer getId() { return ByteBuffer.wrap(id); }
	public void setId(ByteBuffer buf) { 
		this.id = buf.array(); 
	}

	public void setIdByHexString(String hexString) {
		id = BinaryUtils.fromDecimal(hexString);
	}

	@DynamoDBHashKey(attributeName="word")
	public String getWord() { return word; }  
	public void setWord(String word) { this.word = word; }

	@DynamoDBAttribute(attributeName="positions")
	public Set<Integer> getPositions() {
		return  positions;
	}
	public void setPositions(Set<Integer> positions) {
		this.positions = new HashSet<Integer>();
		this.positions.addAll(positions);
	}
	public void addPosition(Integer pos) {
		positions.add(pos);
	}

	@DynamoDBAttribute(attributeName="tf")
	public double getTF() {
		return tf;
	}
	public void setTF(double tf) {
		this.tf = tf;
	}
	
	@DynamoDBAttribute(attributeName="idf")
	public double getIDF() {
		if(idf == null) {
			return -1;
		}
		return idf;
	}
	public void setIDF(double idf) {
		this.idf = idf;
	}
	
	@DynamoDBAttribute(attributeName="pagerank")
	public double getPageRank() {
		if(pagerank == null) {
			return -1;
		}
		return pagerank;
	}
	public void setPageRank(double pagerank) {
		this.pagerank = pagerank;
	}
	
	@DynamoDBAttribute(attributeName="type")
	public int getType() {
		return type;
	}
	public void setType(int type) {
		this.type = type;
	}

	@Override
	public String toString() {
		return word + BinaryUtils.byteArrayToString(id);
	}
	
	@Override
	public boolean equals(Object other) {
		if(other == null || !(other instanceof InvertedIndex)) {
			return false;
		}
		
		InvertedIndex other2 = (InvertedIndex) other;
		if(!other2.word.equals(this.word)) {
			return false;
		}
		if(!Arrays.equals(this.id, other2.id)) {
			return false;
		}
		return true;
	}
	
	@Override
	public int hashCode() {
		return word.hashCode() * 31 + Arrays.hashCode(id);
	}

	public static InvertedIndex parseInput(String line) {
		if (line == null) {
			System.err.println("parseInput: null line!");
			return null;
		}

		String[] splited = line.split("\t");
		if (splited.length != 5) {
			System.err.println("parseInput: bad line: " + line);
			return null;
		}

		String word = splited[0].trim();
		if (word.equals("")) {
			System.err.println("parseInput: word empty: " + line);
			return null;
		}

		byte[] id = BinaryUtils.fromDecimal(splited[1].trim());
		if (id.length == 0) {
			System.err.println("parseInput: id wrong: " + line);
			return null;
		}

		double tf;
		try {
			tf = Double.parseDouble(splited[2].trim());
		} catch(Exception e) {
			System.err.println("parseInput: tf wrong: " + line);
			return null;
		}

		String[] posStrs = splited[3].split(",");
//		if (posStrs.length == 0) {
//			System.err.println("parseInput: positions wrong: " + line);
//			return null;
//		}
		HashSet<Integer> positions = new HashSet<Integer>();
		for (String p : posStrs) {
			try {
				Integer pos = Integer.parseInt(p);
				positions.add(pos);
			} catch(Exception e) {
				System.err.println("parseInput: positions wrong: " + line);
				return null;
			}
		}
		
		String typeStr = splited[4];
		int type;
		try {
			type = Integer.parseInt(typeStr);
		} catch (Exception e) {
			System.err.println("parseInput: type wrong: " + line);
			return null;
		}

		return new InvertedIndex(word, id, tf, positions/*, type*/);
	}
	
	
	/*
	 * hash from word to items that has the word
	 */
	private static HashMap<String, HashSet<InvertedIndex>> items = null;
	private static int countBuffer = 0;
	/**
	 * insert an item of inverted index from parsed input. The item has fields
	 * word, docID, positions, tf, and type, but not idf or pagerank
	 * @param item
	 */
	public static void insert(InvertedIndex item) {
		if(item == null || item.word == null) {
			throw new NullPointerException();
		}
		
		if(items == null) {
			items = new HashMap<String, HashSet<InvertedIndex>>();
		}
		HashSet<InvertedIndex> set = items.get(item.word);
		if (set == null) {
			set = new HashSet<InvertedIndex>();
		}
		set.add(item);
		items.put(item.word, set);
		countBuffer++;
		
		if(items.size() >= 100 || countBuffer >= 1000) { //query to find all idf values of indexes
			List<IDF> idfs = IDF.batchload(items.keySet());
			for(IDF idf : idfs) {
				HashSet<InvertedIndex> iiset = items.get(idf.word); //iiset: InvertedIndexSet
				for(InvertedIndex ii : iiset) {
					ii.idf = idf.idf;
					batchInsert(ii);
				}
			}
		}
	}
	
	private static ArrayList<InvertedIndex> readyItems; //all items ready to be sent for batchsave
	private static void batchInsert(InvertedIndex item) {
		if(readyItems == null) {
			readyItems = new ArrayList<InvertedIndex>();
		}
		readyItems.add(item);
		if(readyItems.size() >= 25) {
			HashSet<ByteBuffer> set = new HashSet<ByteBuffer>();
			for(InvertedIndex i : readyItems) {
				set.add(ByteBuffer.wrap(i.id));
			}
			List<PageRank> results = PageRank.batchload(set);
			for(PageRank p : results) {
				for(InvertedIndex i : readyItems) {
					if(Arrays.equals(i.id, p.id)) {
						i.pagerank = p.rank;
					}
				}
			}
			
			try {
				DynamoTable.mapper.batchSave(readyItems);
			} catch (Exception e) { //if batch save failed, try individul saves
				System.err.println("InvertedIndex.batchInsert: batch failed! try individual save");
				for(InvertedIndex i : readyItems) {
					DynamoTable.mapper.save(i);
				}
			}
			readyItems = null;
		}
	}
	
	public static void createTable() throws InterruptedException {
		CreateTableRequest request = DynamoUtils.createTableHashRange(
				tableName, hashKey, ScalarAttributeType.S, 
				rangeKey, ScalarAttributeType.B, 
				readCapacity, writeCapacity);
		
		DynamoTable.createTable(tableName, request);
	}
	
	/**
	 * populate DB from S3 input
	 */
	public static void populateFromS3(String bucketName, String prefix) {
		long lineCount = 0;
		long lastLineCount = 0;
		long fileCount = 0;
		Date begin = new Date();
		Date last = begin;
		long failedFile = 0;
		long failedLine = 0;
		
		System.out.println("begin to populate InvertedIndex, start date: " + begin.toString());
		S3Iterator iterator = new S3Iterator(bucketName, prefix);
		while(iterator.hasNext()) {
			S3ObjectSummary obj = iterator.next();
			BufferedReader reader = new S3FileReader(obj).getStreamReader();
			if(reader == null) {
				System.out.println("InvertedIndex.populateFromS3: One object can't return inputstream: " + obj.getBucketName() + obj.getKey());
				failedFile++;
				continue;
			}
			String line = null;
			try {
				while((line = reader.readLine()) != null) {
					InvertedIndex item = InvertedIndex.parseInput(line);
					if(item != null) {
						InvertedIndex.insert(item);
					} else {
						failedLine++;
					}
					lineCount++;
				}
			} catch (IOException e1) {
				failedFile++;
				e1.printStackTrace();
				continue;
			}
			fileCount++;
			Date curr = new Date();
			System.out.println("Files done: " + fileCount
					+ "\n\t lines done: " + lineCount
					+ "\n\t failedFile: " + failedFile
					+ "\n\t failedLine: " + failedLine
					+ "\n\t time used for the file: " + TimeUtils.secondsPast(last, curr)
					+ "\n\t average lines per second: " + ((double)(lineCount - lastLineCount)) / TimeUtils.secondsPast(begin, curr));
			last = curr;
			lastLineCount = lineCount;
			
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		IDF.init();
		IDF.populateFromS3("mapreduce-result", "idfmr/part-r-");
		PageRank.init();
		PageRank.populateFromS3("mapreduce-result", "idfmr/part-r-");
		createTable();
		populateFromS3("mapreduce-result", "IndexerResult/part-m-");
	}
}