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
import java.util.List;
import java.util.Map;
import java.util.Set;

import S3.S3FileReader;
import S3.S3Iterator;
import Utils.BinaryUtils;
import Utils.TimeUtils;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * @author dichenli
 * data of page rank
 */
@DynamoDBTable(tableName="PageRank2")
public class PageRank {
	private static Inserter<PageRank> inserter; 
	static String tableName = "PageRank2"; //need to sync with @DynamoDBTable(tableName="xx")
	static String keyName = "id";
	static long readCapacity = 500L; // 10 at most. Or we will be charged
	static long writeCapacity = 1000L; // 10 at most. Or we will be charged
	
	byte[] id; //binary data
	double rank; //page rank

	@DynamoDBHashKey(attributeName="id")
	public ByteBuffer getId() { return ByteBuffer.wrap(id); }

	public void setId(ByteBuffer buf) { 
		this.id = buf.array(); 
	}

	public void setId(String hexString) {
		id = BinaryUtils.fromDecimal(hexString);
	}

	@DynamoDBAttribute(attributeName="rank")
	public double getRank() { return rank; }    
	public void setRank(double rank) { this.rank = rank; }

	@Override
	public String toString() {
		return Arrays.toString(id);  
	}

	@Override
	public boolean equals(Object other) {
		if(other == null || !(other instanceof PageRank)) {
			return false;
		}

		PageRank other2 = (PageRank) other;
		if(!Arrays.equals(this.id, other2.id)) {
			return false;
		}
		return true;
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(id);
	}
	
	public PageRank() {}
	
	public PageRank(String line) {
		parseInput(line);
	}

	public void parseInput(String line) {
		if(line == null) {
			System.out.println("null line");
			throw new IllegalArgumentException();
		}

		String[] splited = line.split("\t");
		if(splited == null || splited.length != 2) {
			System.out.println("bad line: " + line);
			throw new IllegalArgumentException();
		}
		String docID = splited[1];
		double rank;
		try {
			rank = Double.parseDouble(splited[0]);
		} catch(Exception e) {
			e.printStackTrace();
			throw new IllegalArgumentException();
		}

		if(docID.equals("")) {
			System.out.println("Empty line: " + line);
			throw new IllegalArgumentException();
		}

		this.setId(docID);
		this.setRank(rank);
	}
	
	public static void createTable() throws InterruptedException {
		CreateTableRequest request = DynamoUtils.createTableHashKey(
				tableName, keyName, ScalarAttributeType.B, 
				readCapacity, writeCapacity);
		DynamoTable.createTable(tableName, request);
	}

	public static void init() throws InterruptedException {
		createTable();
		inserter = new Inserter<PageRank>();
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

		System.out.println("begin to populate PageRank, start date: " + begin.toString());
		S3Iterator iterator = new S3Iterator(bucketName, prefix);
		while(iterator.hasNext()) {
			S3ObjectSummary obj = iterator.next();
			BufferedReader reader = new S3FileReader(obj).getStreamReader();
			if(reader == null) {
				System.out.println("PageRank.populateFromS3: One object can't return inputstream: " + obj.getBucketName() + obj.getKey());
				failedFile++;
				continue;
			}
			String line = null;
			try {
				while((line = reader.readLine()) != null) {
//					System.out.println(line);
					PageRank item = null;
					try {
						item = new PageRank(line);
//						System.out.println(item);
					} catch (Exception e) {
						failedLine++;
						continue;
					}
					inserter.insert(item);
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

	public static List<PageRank> batchload(Set<ByteBuffer> ids) {

		ArrayList<Object> keys = new ArrayList<Object>();
		for(ByteBuffer id : ids) {
			PageRank key = new PageRank();
			key.id = id.array();
			keys.add(key);
		}

		Map<String, List<Object>> results = DynamoTable.mapper.batchLoad(keys);
		List<Object> PageRankResults = results.get("PageRank");
		if(PageRankResults == null) {
			return new ArrayList<PageRank>(); //empty list
		}
		ArrayList<PageRank> lastResult = new ArrayList<PageRank>();
		for (Object obj : PageRankResults) {
			if ((obj instanceof PageRank)) {
				lastResult.add((PageRank) obj);
			}
		}
		return lastResult;
	}
	
	public static void main(String... args) throws InterruptedException {
		init();
		populateFromS3("mapreduce-result", "pagerank-result/part-r-00000");
	}
	
}
