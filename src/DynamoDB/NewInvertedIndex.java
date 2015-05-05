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
// TODO: Auto-generated Javadoc

/**
 * The Class NewInvertedIndex.
 *
 * @author dichenli
 */
@DynamoDBTable(tableName="InvertedIndex3")
public class NewInvertedIndex {

	/** The table name. */
	static String tableName = "InvertedIndex3"; //need to sync with @DynamoDBTable(tableName="xx")
	
	/** The hash key. */
	static String hashKey = "word";
	
	/** The range key. */
	static String rangeKey = "id";
	
	/** The index. */
	static String index = "tf";
	
	/** The index name. */
	static String indexName = "tfIndex";
	
	/** The read capacity. */
	static long readCapacity = 10L;
	
	/** The write capacity. */
	static long writeCapacity = 10000L;

	/** The inserter. */
	static Inserter<NewInvertedIndex> inserter = new Inserter<NewInvertedIndex>();

	/** The id. */
	byte[] id; //binary data, docID
	
	/** The word. */
	String word; 
	
	/** The positions. */
	HashSet<Integer> positions; //position of the word in document
	
	/** The tf. */
	double tf; //TF value
	//	double idf;
	/** The pagerank. */
	double pagerank;
	
	/** The type. */
	int type;

	/**
	 * Instantiates a new new inverted index.
	 *
	 * @param word2 the word2
	 * @param id2 the id2
	 * @param tf2 the tf2
	 * @param positions2 the positions2
	 * @param type the type
	 */
	public NewInvertedIndex(String word2, byte[] id2, double tf2,
			HashSet<Integer> positions2, int type) {
		this.word = word2;
		this.id = id2;
		this.positions = positions2;
		this.tf = tf2;
		this.type = type;
		//		this.idf = (double)-1;
		this.pagerank = (double)-1;
	}

	/**
	 * Instantiates a new new inverted index.
	 */
	public NewInvertedIndex() {}


	/**
	 * Gets the tf.
	 *
	 * @return the tf
	 */
	@DynamoDBIndexRangeKey(attributeName="tf", localSecondaryIndexName="tfIndex")
	public double getTF() {
		return tf;
	}
	
	/**
	 * Sets the tf.
	 *
	 * @param tf the new tf
	 */
	public void setTF(double tf) {
		this.tf = tf;
	}

	/**
	 * Gets the id.
	 *
	 * @return the id
	 */
	@DynamoDBRangeKey(attributeName="id")
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
	 * Sets the id by hex string.
	 *
	 * @param hexString the new id by hex string
	 */
	public void setIdByHexString(String hexString) {
		id = BinaryUtils.fromDecimal(hexString);
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
	public void setWord(String word) { this.word = word; }

	/**
	 * Gets the positions.
	 *
	 * @return the positions
	 */
	@DynamoDBAttribute(attributeName="positions")
	public Set<Integer> getPositions() {
		return  positions;
	}
	
	/**
	 * Sets the positions.
	 *
	 * @param positions the new positions
	 */
	public void setPositions(Set<Integer> positions) {
		this.positions = new HashSet<Integer>();
		this.positions.addAll(positions);
	}
	
	/**
	 * Adds the position.
	 *
	 * @param pos the pos
	 */
	public void addPosition(Integer pos) {
		positions.add(pos);
	}

	/**
	 * Positions sorted.
	 *
	 * @return the list
	 */
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

	/**
	 * Gets the page rank.
	 *
	 * @return the page rank
	 */
	@DynamoDBAttribute(attributeName="pagerank")
	public double getPageRank() {
		//		if(pagerank == null) {
		//			return -1;
		//		}
		return pagerank;
	}
	
	/**
	 * Sets the page rank.
	 *
	 * @param pagerank the new page rank
	 */
	public void setPageRank(double pagerank) {
		this.pagerank = pagerank;
	}

	/**
	 * Gets the type.
	 *
	 * @return the type
	 */
	@DynamoDBAttribute(attributeName="type")
	public int getType() {
		return type;
	}
	
	/**
	 * Sets the type.
	 *
	 * @param type the new type
	 */
	public void setType(int type) {
		this.type = type;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return word +"\n" + BinaryUtils.byteArrayToDecimalString(id)
				+ "\n" + tf +"\t" + pagerank + "\t" + type;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object other) {
		if(other == null || !(other instanceof NewInvertedIndex)) {
			return false;
		}

		NewInvertedIndex other2 = (NewInvertedIndex) other;
		if(!other2.word.equals(this.word)) {
			return false;
		}
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
		return word.hashCode() * 31 + Arrays.hashCode(id);
	}

	/**
	 * Positions sorted.
	 *
	 * @return the list
	 */
	public List<Integer> positionsSorted() {
		if(positions == null) {
			return null;
		}
		Integer[] arr = positions.toArray(new Integer[0]);
		Arrays.sort(arr);
		return Arrays.asList(arr);
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
					NewInvertedIndex item = NewInvertedIndex.parseInput(line);
					line = reader.readLine();
					if(line == null) {
						NewInvertedIndex.insert(item, true);
						break;
					}
					if(item != null) {
						NewInvertedIndex.insert(item, false);
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

	/**
	 * Parses the input.
	 *
	 * @param line the line
	 * @return the new inverted index
	 */
	public static NewInvertedIndex parseInput(String line) {
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

		return new NewInvertedIndex(word, id, tf, positions, type);
	}


	/*
	 * hash from word to items that has the word
	 */
	/** The items. */
	private static HashMap<String, HashSet<NewInvertedIndex>> items = null;
	
	/** The count buffer. */
	private static int countBuffer = 0;
	
	/**
	 * insert an item of inverted index from parsed input. The item has fields
	 * word, docID, positions, tf, and type, but not idf or pagerank
	 *
	 * @param item the item
	 * @param flush force dump buffer to DB
	 */
	public static void insert(NewInvertedIndex item, boolean flush) {
		//		System.out.println("======insert: \n" + item);
		if(item == null || item.word == null) {
			return;
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

	/** The ready items. */
	private static ArrayList<NewInvertedIndex> readyItems; //all items ready to be sent for batchsave
	
	/**
	 * Batch insert.
	 *
	 * @param item the item
	 * @param flush the flush
	 */
	private static void batchInsert(NewInvertedIndex item, boolean flush) {
		//		System.out.println("======BatchInsert: \n" + item);
		if(readyItems == null) {
			readyItems = new ArrayList<NewInvertedIndex>();
		}
		readyItems.add(item);
		if(readyItems.size() >= 25 || flush) {
			//			System.out.println("batchInsert: ready to flush");
			HashSet<ByteBuffer> set = new HashSet<ByteBuffer>();
			for(NewInvertedIndex i : readyItems) {
				set.add(ByteBuffer.wrap(i.id));
			}
			//			System.out.println("batchInsert: set for batch load pagerank size: " + set.size());
			List<PageRank> results = PageRank.batchload(set);
			for(NewInvertedIndex i : readyItems) {
				for(PageRank p : results) {
					if(Arrays.equals(i.id, p.id)) {
						i.pagerank = p.rank;
						//						System.out.println("====Found PageRank " + i);
						break;
					}
				}
			}
			//			System.out.println("batchsave, readyItems size: " + readyItems.size());
			for(NewInvertedIndex i : readyItems) {
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

	/**
	 * Creates the table.
	 *
	 * @throws InterruptedException the interrupted exception
	 */
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
		PageRank.createTable();
	}

	/**
	 * query by the given word, order the results by tf from high to low.
	 *
	 * @param word the word
	 * @return the paginated query list
	 */
	public static PaginatedQueryList<NewInvertedIndex> query(String word) {
		NewInvertedIndex item = new NewInvertedIndex();
		item.setWord(word);
		DynamoDBQueryExpression<NewInvertedIndex> queryExpression 
		= new DynamoDBQueryExpression<NewInvertedIndex>().withHashKeyValues(item)
		.withIndexName(indexName).withScanIndexForward(false); //query by index of TF, ordered from high to low

		PaginatedQueryList<NewInvertedIndex> collection 
		= DynamoTable.mapper.query(NewInvertedIndex.class, queryExpression);
		return collection;
	}



	/** The job. */
	public static String job = "";
	
	/**
	 * Run distributed.
	 *
	 * @param args the args
	 * @throws Exception the exception
	 */
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

	/**
	 * Run remaining.
	 *
	 * @param number the number
	 * @throws Exception the exception
	 */
	public static void runRemaining(int number) throws Exception {
		int[] front = {144, 161, 168, 179, 52, 53, 54, 55, 104, 105, 106, 107};
		ArrayList<Integer> remaining = new ArrayList<Integer>();
		for (int i = 0; i < front.length; i++) {
			for (int j = front[i]; j <= 226; j += 16) {
				remaining.add(j);
			}
		}
		String bucket = "mapreduce-result";
		//		String numberStr = args[0];
		//		int number = Integer.parseInt(numberStr);
		createTable();
		System.out.println("Crawler#" + number);
		for (int i = 0; i < remaining.size(); i++) {
			if (i % 12 == number) {	
				//				System.out.println("\t" + remaining.get(i));
				job += "|" + remaining.get(i);
				String digit = "000" + remaining.get(i);
				digit = digit.substring(digit.length() - 3, digit.length());
				try {
					populateFromS3("mapreduce-result", "IndexerResult/part-m-00" + digit);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Run list.
	 *
	 * @throws Exception the exception
	 */
	public static void runList() throws Exception {
		int[] jobs = {120, 185, 123};
		String bucket = "mapreduce-result";
		//		String numberStr = args[0];
		//		int number = Integer.parseInt(numberStr);
		createTable();
		for (int j : jobs) {
			//				System.out.println("\t" + remaining.get(i));
			job += "|" + j;
			String digit = "000" + j;
			digit = digit.substring(digit.length() - 3, digit.length());
			try {
				populateFromS3("mapreduce-result", "IndexerResult/part-m-00" + digit);
			} catch (Exception e) {
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
	public static void main(String[] args) throws Exception {
		//		IDF.init();
		//		IDF.populateFromS3("mapreduce-result", "idfmr/part-r-");
		//		PageRank.init();
		//		PageRank.populateFromS3("mapreduce-result", "pagerank-result/part-r-");

		//		createTable();
		//		populateFromS3("mapreduce-result", "IndexerResult/part-m-00");
		//		runDistributed(args);

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
		//		List<InvertedIndex> results = query("newsroom");
		//		for(InvertedIndex ii : results) {
		//			System.out.println(ii);
		//		}
		//		for(int i = 0; i < 12; i++)
		//			runRemaining(i);
		runList();
		//		String line = "editor	37087316027811206319887670560891285046980393525	-1	,	3";
		//		InvertedIndex result = InvertedIndex.parseInput(line);
		//		System.out.println(result.getType());
		//		System.out.println(result.getTF());
		//		System.out.println(result.getWord());
		//		System.out.println(result.getPositions());
	}
}


//Crawler3:	|192|179|196|213|55|120	1827771
//120|185|123

