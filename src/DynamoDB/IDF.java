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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import S3.S3FileReader;
import S3.S3Iterator;
import Utils.BinaryUtils;
import Utils.TimeUtils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.s3.model.S3ObjectSummary;

// TODO: Auto-generated Javadoc
/**
 * The Class IDF.
 *
 * @author dichenli
 * data of page idf
 */
@DynamoDBTable(tableName="IDF2")
public class IDF extends Item{

	/** The table name. */
	static String tableName = "IDF2"; //need to sync with @DynamoDBTable(tableName="xx")
	
	/** The key name. */
	static String keyName = "word";
	
	/** The read capacity. */
	static long readCapacity = 500L; // 10 at most. Or we will be charged
	
	/** The write capacity. */
	static long writeCapacity = 1000L; // 10 at most. Or we will be charged
	
	/** The inserter. */
	static Inserter<IDF> inserter;

	/** The word. */
	String word; //binary data
	
	/** The idf. */
	double idf; //page idf

	/**
	 * Instantiates a new idf.
	 */
	public IDF() {
		super();
	}

	/**
	 * Gets the word.
	 *
	 * @return the word
	 */
	@DynamoDBHashKey(attributeName="word")
	public String getWord() { return word; }
	
	/**
	 * Sets the word.
	 *
	 * @param word the new word
	 */
	public void setWord(String word) {
		this.word = word;
	}

	/**
	 * Gets the idf.
	 *
	 * @return the idf
	 */
	@DynamoDBAttribute(attributeName="idf")
	public double getidf() { return idf; }    
	
	/**
	 * Sets the idf.
	 *
	 * @param idf the new idf
	 */
	public void setidf(double idf) {
		this.idf = idf;
	}

	/**
	 * Instantiates a new idf.
	 *
	 * @param line the line
	 */
	public IDF(String line) {
		super(line);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return word + idf;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object other) {
		if(other == null || !(other instanceof IDF)) {
			return false;
		}

		IDF other2 = (IDF) other;
		if(!other2.word.equals(this.word)) {
			return false;
		}
		return this.word.equals(other2.word);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return word.hashCode();
	}


	/* (non-Javadoc)
	 * @see DynamoDB.Item#parse(java.lang.String)
	 */
	@Override
	public void parse(String line) {
		if(line == null) {
			System.out.println("null line");
			throw new NullPointerException();
		}

		String[] splited = line.split("\t");
		if(splited == null || splited.length != 2) {
			System.out.println("bad line: " + line);
			throw new IllegalArgumentException();
		}
		double idf;
		try {
			idf = Double.parseDouble(splited[1]);
		} catch(Exception e) {
			e.printStackTrace();
			throw new IllegalArgumentException();
		}

		String word = splited[0];
		if(word.equals("")) {
			System.out.println("Empty line: " + line);
			throw new IllegalArgumentException();
		}

		this.word = word;
		this.idf = idf;
	}

	//    public static IDF parseInput(String line) {
	//		if(line == null) {
	//			System.out.println("null line");
	//			return null;
	//		}
	//		
	//		String[] splited = line.split("\t");
	//		if(splited == null || splited.length != 2) {
	//			System.out.println("bad line: " + line);
	//			return null;
	//		}
	//		double idf;
	//		try {
	//			idf = Double.parseDouble(splited[1]);
	//		} catch(Exception e) {
	//			e.printStackTrace();
	//			return null;
	//		}
	//		
	//		String word = splited[0];
	//		if(word.equals("")) {
	//			System.out.println("Empty line: " + line);
	//			return null;
	//		}
	//		
	//		IDF item = new IDF();
	//		item.word = word;
	//		item.idf = idf;
	//		return item;
	//	}

	/**
	 * Load.
	 *
	 * @param word the word
	 * @return the idf
	 * @throws Exception the exception
	 */
	public static IDF load(String word) throws Exception {
		if (DynamoTable.mapper == null) {
			DynamoTable.init();
		}
		return DynamoTable.mapper.load(DynamoDB.IDF.class, word);
	}

	/**
	 * Batchload.
	 *
	 * @param words the words
	 * @return the list
	 */
	public static List<IDF> batchload(Set<String> words) {
		//		System.out.println("batchload IDF words size: " + words.size());
		ArrayList<Object> keys = new ArrayList<Object>();
		for(String word : words) {
			IDF key = new IDF();
			key.word = word;
			//			System.out.println("batchload IDF: " + word);
			keys.add(key);
		}
		
		try {
			Map<String, List<Object>> results = DynamoTable.mapper.batchLoad(keys);
			List<Object> idfResults = results.get(tableName);
			if(idfResults == null) {
				System.out.println("batchload IDF: no results");
				return new ArrayList<IDF>(); //empty
			}
			ArrayList<IDF> lastResult = new ArrayList<IDF>();
			for (Object obj : idfResults) {
				if ((obj instanceof IDF)) {
					lastResult.add((IDF) obj);
				}
			}
			return lastResult;
		}  catch (AmazonServiceException ase) { //if batch load failed, try single loads instead
			System.out.println("One or more parameter values were invalid: "
					+ "Size of hashkey has exceeded the maximum size limit "
					+ "of2048 bytes");
			
			ArrayList<IDF> lastResult = new ArrayList<IDF>();
			for(Object word :  keys) {
				try {
					IDF result = load((String)word);
					lastResult.add(result);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			return lastResult;
		}
	}

	/**
	 * Creates the table.
	 *
	 * @throws InterruptedException the interrupted exception
	 */
	public static void createTable() throws InterruptedException {
		CreateTableRequest request = DynamoUtils.createTableHashKey(
				tableName, keyName, ScalarAttributeType.S, 
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
		inserter = new Inserter<IDF>();
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

		System.out.println("begin to populate IDF, start date: " + begin.toString());
		S3Iterator iterator = new S3Iterator(bucketName, prefix);
		while(iterator.hasNext()) {
			S3ObjectSummary obj = iterator.next();
			BufferedReader reader = new S3FileReader(obj).getStreamReader();
			if(reader == null) {
				System.out.println("IDF.populateFromS3: One object can't return inputstream: " + obj.getBucketName() + obj.getKey());
				failedFile++;
				continue;
			}
			String line = null;
			try {
				while((line = reader.readLine()) != null) {
					IDF item = null;
					try {
						item = new IDF(line);
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
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws Exception the exception
	 */
	public static void main(String... args) throws Exception {
		//		HashSet<String> words = new HashSet<String>();
		//		words.add("anyth");
		//		words.add("kite");
		//		List<IDF> results = IDF.batchload(words);
		//		for(IDF i : results) {
		//			System.out.println(i);
		//		}

		init();
		//		populateFromS3("mapreduce-result", "idfmr/part-r-00000");
		IDF item = new IDF();
		HashSet<String> words = new HashSet<String>();
		words.add("main");
		List<IDF> results = batchload(words);
		for(IDF r : results) {
			System.out.println(r);
		}
	}

}
