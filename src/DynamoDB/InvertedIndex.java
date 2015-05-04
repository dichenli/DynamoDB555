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
import java.util.Set;

import S3.S3FileReader;
import S3.S3Iterator;
import Utils.BinaryUtils;
import Utils.TimeUtils;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIndexRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper.FailedBatch;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedQueryList;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.s3.model.S3ObjectSummary;
/**
 * @author dichenli
 *
 */
@DynamoDBTable(tableName="InvertedIndex2")
public class InvertedIndex {

	static String tableName = "InvertedIndex2"; //need to sync with @DynamoDBTable(tableName="xx")
	static String hashKey = "word";
	static String rangeKey = "id";
	static String index = "tf";
	static String indexName = "tfIndex";
	static long readCapacity = 10L;
	static long writeCapacity = 10000L;
	
	static Inserter<InvertedIndex> inserter = new Inserter<InvertedIndex>();

	byte[] id; //binary data, docID
	String word; 
	HashSet<Integer> positions; //position of the word in document
	double tf; //TF value
//	double idf;
	double pagerank;
	int type;
	
	public InvertedIndex(String word2, byte[] id2, double tf2,
			HashSet<Integer> positions2, int type) {
		this.word = word2;
		this.id = id2;
		this.positions = positions2;
		this.tf = tf2;
		this.type = type;
//		this.idf = (double)-1;
		this.pagerank = (double)-1;
	}
	
	public InvertedIndex() {}
	
	
	@DynamoDBIndexRangeKey(attributeName="tf", localSecondaryIndexName="tfIndex")
	public double getTF() {
		return tf;
	}
	public void setTF(double tf) {
		this.tf = tf;
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
	
	public List<Integer> PositionsSorted() {
		if(positions == null) {
			return null;
		}
		Integer[] arr = positions.toArray(new Integer[0]);
		Arrays.sort(arr);
		return Arrays.asList(arr);
	}
	
//	@DynamoDBAttribute(attributeName="PositionsSorted")
	

//	@DynamoDBAttribute(attributeName="idf")
//	public double getIDF() {
////		if(idf == null) {
////			return -1;
////		}
//		return idf;
//	}
//	public void setIDF(double idf) {
//		this.idf = idf;
//	}

	@DynamoDBAttribute(attributeName="pagerank")
	public double getPageRank() {
//		if(pagerank == null) {
//			return -1;
//		}
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
		return word +"\n" + BinaryUtils.byteArrayToString(id)
				+ "\n" + tf +"\t" + pagerank + "\t" + type;
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
	
	public List<Integer> positionsSorted() {
		if(positions == null) {
			return null;
		}
		Integer[] arr = positions.toArray(new Integer[0]);
		Arrays.sort(arr);
		return Arrays.asList(arr);
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
		HashSet<Integer> positions = null;
		for (String p : posStrs) {
			if(positions == null) {
				positions = new HashSet<Integer>();
			}
			try {
				Integer pos = Integer.parseInt(p);
				positions.add(pos);
			} catch(Exception e) {
				System.err.println("parseInput: positions wrong: " + line);
				return null;
			}
		}

		String typeStr = splited[4];
		int type = -1;
		try {
			type = Integer.parseInt(typeStr);
		} catch (Exception e) {
			System.err.println("parseInput: type wrong: " + line);
			return null;
		}

		return new InvertedIndex(word, id, tf, positions, type);
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
	 * @param flush force dump buffer to DB
	 */
	public static void insert(InvertedIndex item, boolean flush) {
//		System.out.println("======insert: \n" + item);
		if(item == null || item.word == null) {
			throw new NullPointerException();
		}
		batchInsert(item, flush);
/*************Join IDF Cancelled**************/
//		if(items == null) {
//			items = new HashMap<String, HashSet<InvertedIndex>>();
//		}
//		HashSet<InvertedIndex> set = items.get(item.word);
//		if (set == null) {
//			set = new HashSet<InvertedIndex>();
//		}
//		set.add(item);
//		items.put(item.word, set);
//		countBuffer++;
//
//		if(items.keySet().size() >= 100 || flush) { //query to find all idf values of indexes
//			List<IDF> idfs = IDF.batchload(items.keySet());
//			System.out.println("batchload idfs size: " + idfs.size());
//			System.out.println("batchload idfs, items.keyset size: " + items.keySet().size());
//			for(IDF idf : idfs) {
//				HashSet<InvertedIndex> iiset = items.get(idf.word); //iiset: InvertedIndexSet
//				for(InvertedIndex ii : iiset) {
//					ii.idf = idf.idf;
//					System.out.println("====After IDF====\n" + ii);
////					batchInsert(ii, flush);
//				}
//			}
//			for(HashSet<InvertedIndex> iiset : items.values()) {
//				for(InvertedIndex ii : iiset) {
//					System.out.println("Call batchInsert: " + ii);
//					batchInsert(ii, flush);
//				}
//			}
//			items = null;
//			countBuffer = 0;
//		}
	}

	private static ArrayList<InvertedIndex> readyItems; //all items ready to be sent for batchsave
	private static void batchInsert(InvertedIndex item, boolean flush) {
//		System.out.println("======BatchInsert: \n" + item);
		if(readyItems == null) {
			readyItems = new ArrayList<InvertedIndex>();
		}
		readyItems.add(item);
		if(readyItems.size() >= 100 || flush) {
//			System.out.println("batchInsert: ready to flush");
			HashSet<ByteBuffer> set = new HashSet<ByteBuffer>();
			for(InvertedIndex i : readyItems) {
				set.add(ByteBuffer.wrap(i.id));
			}
//			System.out.println("batchInsert: set for batch load pagerank size: " + set.size());
			List<PageRank> results = PageRank.batchload(set);
			for(InvertedIndex i : readyItems) {
				for(PageRank p : results) {
					if(Arrays.equals(i.id, p.id)) {
						i.pagerank = p.rank;
//						System.out.println("====Found PageRank " + i);
						break;
					}
				}
			}
//			System.out.println("batchsave, readyItems size: " + readyItems.size());
			for(InvertedIndex i : readyItems) {
				inserter.insert(i, flush);
			}
//			try {
//				List<FailedBatch> failed = DynamoTable.mapper.batchSave(readyItems);
//			} catch (Exception e) { //if batch save failed, try individul saves
//				System.err.println("InvertedIndex.batchInsert: batch failed! try individual save");
//				for(InvertedIndex i : readyItems) {
//					DynamoTable.mapper.save(i);
//				}
//			}
			readyItems = null;
		}
	}

	public static void createTable() throws InterruptedException {
		CreateTableRequest request = DynamoUtils.createTableHashRange(
				tableName, hashKey, ScalarAttributeType.S, 
				rangeKey, ScalarAttributeType.B, 
				readCapacity, writeCapacity);
		
		request
		.withAttributeDefinitions(new AttributeDefinition().withAttributeName(index).withAttributeType(ScalarAttributeType.N));
//		.withAttributeDefinitions(new AttributeDefinition().withAttributeName("pagerank").withAttributeType(ScalarAttributeType.N))
//		.withAttributeDefinitions(new AttributeDefinition().withAttributeName("type").withAttributeType(ScalarAttributeType.N));
		
		ArrayList<KeySchemaElement> indexKeySchema = new ArrayList<KeySchemaElement>();
		indexKeySchema.add(new KeySchemaElement().withAttributeName("word").withKeyType(KeyType.HASH));
		indexKeySchema.add(new KeySchemaElement().withAttributeName(index).withKeyType(KeyType.RANGE));
		Projection projection = new Projection().withProjectionType(ProjectionType.ALL);
		
		LocalSecondaryIndex localSecondaryIndex = new LocalSecondaryIndex()
	    .withIndexName(indexName).withKeySchema(indexKeySchema)
	    .withProjection(projection);

		request.withLocalSecondaryIndexes(localSecondaryIndex);

		DynamoTable.createTable(tableName, request);
	}
	
	public static PaginatedQueryList<InvertedIndex> query(String word) {
		InvertedIndex item = new InvertedIndex();
		item.setWord(word);
		DynamoDBQueryExpression<InvertedIndex> queryExpression 
		= new DynamoDBQueryExpression<InvertedIndex>().withHashKeyValues(item)
		.withIndexName(indexName).withScanIndexForward(false);
		
		PaginatedQueryList<InvertedIndex> collection 
		= DynamoTable.mapper.query(InvertedIndex.class, queryExpression);
		return collection;
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
				line = reader.readLine();
				while(true) {
					InvertedIndex item = InvertedIndex.parseInput(line);
					line = reader.readLine();
					if(line == null) {
						InvertedIndex.insert(item, true);
						break;
					}
					if(item != null) {
						InvertedIndex.insert(item, false);
						System.out.println(job + "\t" + lineCount);
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
			System.err.println("Files done: " + fileCount
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

	public static String job = "";
	public static void runDistributed(String[] args) throws Exception {
		String bucket = "mapreduce-result";
		String numberStr = args[0];
		int number = Integer.parseInt(numberStr);
		createTable();
		for(int i = 0; i <= 226; i++) {
			if(i % 16 == number) {		
				job += "|" + i;
				String digit = "000" + i;
				digit = digit.substring(digit.length() - 3, digit.length());
				populateFromS3("mapreduce-result", "IndexerResult/part-m-00" + digit);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		//		IDF.init();
		//		IDF.populateFromS3("mapreduce-result", "idfmr/part-r-");
		//		PageRank.init();
		//		PageRank.populateFromS3("mapreduce-result", "pagerank-result/part-r-");
		
		createTable();
//		populateFromS3("mapreduce-result", "IndexerResult/part-m-00");
		runDistributed(args);
		
//		int[] tasks = {171, 187, 203, 219, 218, 214, 175, 191, 207, 223};
//		String bucket = "mapreduce-result";
//		String numberStr = args[0];
//		int number = Integer.parseInt(numberStr);
//		createTable();
//		for(int i = 0; i < tasks.length; i++) {
//			if(i % 10 == number) {		
//				job += "|" + tasks[i];
//				String digit = "000" + tasks[i];
//				digit = digit.substring(digit.length() - 3, digit.length());
//				populateFromS3("mapreduce-result", "IndexerResult/part-m-00" + digit);
//			}
//		}
		
//		createTable();
//		InvertedIndex i = parseInput("mosdafafadsfw	291647802747036241376099890398414543841464994659	1.5	,	3");
//		System.out.println(i);
//		insert(i, false);
//		DynamoTable.mapper.save(i);
//		InvertedIndex i2 = parseInput("featursdfsdf	132937547224450410377374508132505655139101005397	0.8	12	0");
//		System.out.println(i2);
//		insert(i2, false);
//		DynamoTable.mapper.save(i2);
//		InvertedIndex i3 = parseInput("featurddsdfsdf	132937548124450410377374508132505655139101005397	0.9	12	0");
//		System.out.println(i3);
//		insert(i3, true);
//		DynamoTable.mapper.save(i2);
//		List<InvertedIndex> results = query("featurddsdfsdf");
//		for(InvertedIndex ii : results) {
//			System.out.println(ii);
//		}

//		String line = "editor	37087316027811206319887670560891285046980393525	-1	,	3";
//		InvertedIndex result = InvertedIndex.parseInput(line);
//		System.out.println(result.getType());
//		System.out.println(result.getTF());
//		System.out.println(result.getWord());
//		System.out.println(result.getPositions());
	}
}
