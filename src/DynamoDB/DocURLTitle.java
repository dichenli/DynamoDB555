package DynamoDB;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import S3.S3FileReader;
import S3.S3Iterator;
import Utils.BinaryUtils;
import Utils.TimeUtils;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.s3.model.S3ObjectSummary;
// TODO: Auto-generated Javadoc

/**
 * Object Persistent model, to populate docID-URL table .
 *
 * @author dichenli
 */
@DynamoDBTable(tableName="DocURLTitle")
public class DocURLTitle {
	
	/** The table name. */
	static String tableName = "DocURLTitle"; //need to sync with @DynamoDBTable(tableName="xx")
	
	/** The key name. */
	static String keyName = "id";
	
	/** The read capacity. */
	static long readCapacity = 1L;
	
	/** The write capacity. */
	static long writeCapacity = 5000L;
	
	/** The inserter. */
	static Inserter<DocURLTitle> inserter;
	
	/** The id. */
	byte[] id; //binary data
	
	/** The url. */
	String url;
	
	/** The title. */
	String title;
	
	/**
	 * Instantiates a new doc url title.
	 */
	public DocURLTitle() {}
	
	/**
	 * Instantiates a new doc url title.
	 *
	 * @param decimalID the decimal id
	 * @param url the url
	 * @param title the title
	 */
	public DocURLTitle(String decimalID, String url, String title) {
		this.id = BinaryUtils.fromDecimal(decimalID);
		this.url = url;
		this.title = title;
	}
	
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
     * Id set by string.
     *
     * @param decimalString the decimal string
     */
    public void idSetByString(String decimalString) {
    	id = BinaryUtils.fromDecimal(decimalString);
    }
    
    /**
     * Gets the url.
     *
     * @return the url
     */
    @DynamoDBAttribute(attributeName="url")
    public String getURL() { return url; }    
    
    /**
     * Sets the url.
     *
     * @param url the new url
     */
    public void setURL(String url) { this.url = url; }
    
    /**
     * Gets the title.
     *
     * @return the title
     */
    @DynamoDBAttribute(attributeName="title")
    public String getTitle() { return title; }    
    
    /**
     * Sets the title.
     *
     * @param title the new title
     */
    public void setTitle(String title) { this.title = title; }
    
    
    
    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
       return url;  
    }
    
    /**
     * Parses the input.
     *
     * @param line the line
     * @return the doc url title
     */
    public static DocURLTitle parseInput(String line) {
		if(line == null) {
			System.out.println("null line");
			return null;
		}
		
		String[] splited = line.split("\t", 3);
		if(splited == null || splited.length != 3) {
			System.out.println("bad line: " + line);
			return null;
		}
		String docID = splited[0];
		String url = splited[1];
		String title = splited[2];
		if(docID.equals("") || url.equals("") || title.equals("")) {
			System.out.println("empty content: " + line);
			return null;
		}
		if(title.length() > 200) {
			title = title.substring(0, 200);
		}
		
		DocURLTitle item = new DocURLTitle();
		item.idSetByString(docID);
		item.setURL(url);
		item.setTitle(title);
		return item;
	}
    
    /**
     * Load from byte buffer.
     *
     * @param bytes the bytes
     * @return the doc url title
     * @throws Exception the exception
     */
    public static DocURLTitle loadFromByteBuffer(ByteBuffer bytes) throws Exception {
    	return load(bytes.array());
    }
    
    /**
     * Load from decimal string.
     *
     * @param decimalStr the decimal str
     * @return the doc url title
     * @throws Exception the exception
     */
    public static DocURLTitle loadFromDecimalString(String decimalStr) throws Exception {
    	return load(BinaryUtils.fromDecimal(decimalStr));
    }
    
    /**
     * Load.
     *
     * @param id the id
     * @return the doc url title
     * @throws Exception the exception
     */
    public static DocURLTitle load(byte[] id) throws Exception {
    	if (DynamoTable.mapper == null) {
    		DynamoTable.init();
    	}
    	return DynamoTable.mapper.load(DocURLTitle.class, ByteBuffer.wrap(id));
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
		inserter = new Inserter<DocURLTitle>();
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

		System.out.println("begin to populate DocURLTitle, start date: " + begin.toString());
		S3Iterator iterator = new S3Iterator(bucketName, prefix);
		while(iterator.hasNext()) {
			S3ObjectSummary obj = iterator.next();
			BufferedReader reader = new S3FileReader(obj).getStreamReader();
			if(reader == null) {
				System.out.println("DocURLTitle.populateFromS3: One object can't return inputstream: " + obj.getBucketName() + obj.getKey());
				failedFile++;
				continue;
			}
			String line = null;
			try {
				while((line = reader.readLine()) != null) {
					DocURLTitle item = null;
					try {
						item = parseInput(line);
						if(item == null) {
							failedLine++;
							continue;
						}
					} catch (Exception e) {
						failedLine++;
						continue;
					}
					inserter.insert(item);
					lineCount++;
					System.out.println(job + "\t" + lineCount);
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
    
	/** The job. */
	static String job = "";
    
    /**
     * The main method.
     *
     * @param args the arguments
     * @throws Exception the exception
     */
    public static void main(String... args) throws Exception {
    	init();
    	
    	int fileCount = 7;
    	int nodeCount = 3;
    	
    	String bucket = "mapreduce-result";
    	String prefix = "title-result/part-r-00";
		String numberStr = args[0];
		int number = Integer.parseInt(numberStr);
		createTable();
		for(int i = 0; i <= fileCount; i++) {
			if(i % nodeCount == number) {		
				job += "|" + i;
				String digit = "000" + i;
				digit = digit.substring(digit.length() - 3, digit.length());
				populateFromS3(bucket, prefix + digit);
			}
		}
//    	System.out.println(loadFromDecimalString("478265070481920712437327189905938532370961602507"));
    	
    }

}
