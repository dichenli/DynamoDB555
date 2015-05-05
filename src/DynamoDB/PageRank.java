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

// TODO: Auto-generated Javadoc
/**
 * The Class PageRank.
 *
 * @author dichenli
 * data of page rank
 */
@DynamoDBTable(tableName="PageRank2")
public class PageRank {
	
	/** The inserter. */
	private static Inserter<PageRank> inserter; 
	
	/** The table name. */
	static String tableName = "PageRank2"; //need to sync with @DynamoDBTable(tableName="xx")
	
	/** The key name. */
	static String keyName = "id";
	
	/** The read capacity. */
	static long readCapacity = 500L; // 10 at most. Or we will be charged
	
	/** The write capacity. */
	static long writeCapacity = 1000L; // 10 at most. Or we will be charged
	
	/** The id. */
	byte[] id; //binary data
	
	/** The rank. */
	double rank; //page rank

	/**
	 * Gets the id.
	 *
	 * @return the id
	 */
	@DynamoDBHashKey(attributeName="id")
	public ByteBuffer getId() { return ByteBuffer.wrap(id); }

	/**
	 * Sets the id.
	 *
	 * @param buf the new id
	 */
	public void setId(ByteBuffer buf) { 
		this.id = buf.array(); 
	}

	/**
	 * Sets the id.
	 *
	 * @param hexString the new id
	 */
	public void setId(String hexString) {
		id = BinaryUtils.fromDecimal(hexString);
	}

	/**
	 * Gets the rank.
	 *
	 * @return the rank
	 */
	@DynamoDBAttribute(attributeName="rank")
	public double getRank() { return rank; }    
	
	/**
	 * Sets the rank.
	 *
	 * @param rank the new rank
	 */
	public void setRank(double rank) { this.rank = rank; }

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return Arrays.toString(id);  
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
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

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return Arrays.hashCode(id);
	}
	
	/**
	 * Instantiates a new page rank.
	 */
	public PageRank() {}
	
	/**
	 * Instantiates a new page rank.
	 *
	 * @param line the line
	 */
	public PageRank(String line) {
		parseInput(line);
	}

	/**
	 * Parses the input.
	 *
	 * @param line the line
	 */
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
	
	/**
	 * Creates the table.
	 *
	 * @throws InterruptedException the interrupted exception
	 */
	public static void createTable() throws InterruptedException {
		CreateTableRequest request = DynamoUtils.createTableHashKey(
				tableName, keyName, ScalarAttributeType.B, 
				readCapacity, writeCapacity);
		DynamoTable.createTable(tableName, request);
	}

	/**
	 * Inits the.
	 *
	 * @throws InterruptedException the interrupted exception
	 */
	public static void init() throws InterruptedException {
		createTable();
		inserter = new Inserter<PageRank>();
	}
	
	/**
	 * populate DB from S3 input.
	 *
	 * @param bucketName the bucket name
	 * @param prefix the prefix
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

	/**
	 * Batchload.
	 *
	 * @param ids the ids
	 * @return the list
	 */
	public static List<PageRank> batchload(Set<ByteBuffer> ids) {

		ArrayList<Object> keys = new ArrayList<Object>();
		for(ByteBuffer id : ids) {
			PageRank key = new PageRank();
			key.id = id.array();
			keys.add(key);
		}

		Map<String, List<Object>> results = DynamoTable.mapper.batchLoad(keys);
		List<Object> PageRankResults = results.get(tableName);
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
	
	/**
	 * not tested!!.
	 *
	 * @param decimalID the decimal id
	 * @return the page rank
	 * @throws Exception the exception
	 */
	public static PageRank load(String decimalID) throws Exception {
		if (DynamoTable.mapper == null) {
			DynamoTable.init();
		}
		
		byte[] id = BinaryUtils.fromDecimal(decimalID);
		return DynamoTable.mapper.load(DynamoDB.PageRank.class, ByteBuffer.wrap(id));
	}
	
	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws Exception the exception
	 */
	public static void main(String... args) throws Exception {
		init();
//		populateFromS3("mapreduce-result", "pagerank-result/part-r-00000");
	}
	
}
