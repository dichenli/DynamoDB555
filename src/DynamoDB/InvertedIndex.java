/**
 * 
 */
package DynamoDB;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;

import S3.S3FileReader;
import S3.S3Iterator;
import Utils.BinaryUtils;
import Utils.IOUtils;
import Utils.TimeUtils;

import com.amazonaws.services.dynamodbv2.datamodeling.*;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
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
			HashSet<Integer> positions2, int type) {
		this.word = word2;
		this.id = id2;
		this.positions = positions2;
		this.tf = tf2;
		this.type = type;
		this.idf = (double)-1;
		this.pagerank = (double)-1;
	}

	public InvertedIndex() {}

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

	public void setPositionsSorted(List<Integer> positions) {

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

	public List<Integer> PositionsSorted() {
		if(positions == null) {
			return new ArrayList<Integer>();
		}
		Integer[] arr = positions.toArray(new Integer[0]);
		Arrays.sort(arr);
		return Arrays.asList(arr);
	}

	public void setType(int type) {
		this.type = type;
	}

	@Override
	public String toString() {
		return word +"\n" + BinaryUtils.byteArrayToDecimalString(id)
				+"\n" + idf + "\t" + pagerank + "\t" + type;
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

	public List<Integer> getPositionsSorted() {
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
	 */
	public static void insert(InvertedIndex item) {
		//		System.out.println("======insert: \n" + item);
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

		if(items.keySet().size() >= 100) { //query to find all idf values of indexes
			List<IDF> idfs = IDF.batchload(items.keySet());
			//			System.out.println("batchload idfs size: " + idfs.size());
			//			System.out.println("batchload idfs, items.keyset size: " + items.keySet().size());
			for(IDF idf : idfs) {
				HashSet<InvertedIndex> iiset = items.get(idf.word); //iiset: InvertedIndexSet
				for(InvertedIndex ii : iiset) {
					ii.idf = idf.idf;
					//					System.out.println("====After IDF====\n" + ii);
					batchInsert(ii);
				}
			}
			for(HashSet<InvertedIndex> iiset : items.values()) {
				for(InvertedIndex ii : iiset) {
					batchInsert(ii);
				}
			}
			items = null;
			countBuffer = 0;
		}
	}

	private static ArrayList<InvertedIndex> readyItems; //all items ready to be sent for batchsave
	private static void batchInsert(InvertedIndex item) {
		//		System.out.println("======BatchInsert: \n" + item);
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
			for(InvertedIndex i : readyItems) {
				for(PageRank p : results) {
					if(Arrays.equals(i.id, p.id)) {
						i.pagerank = p.rank;
						//						System.out.println("====After PageRank====\n" + i);
						break;
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

	public static PaginatedQueryList<InvertedIndex> query(String word) {
		return query(word, null, null);
	}

	public static PaginatedQueryList<InvertedIndex> query(
			String word, DynamoDBQueryExpression<InvertedIndex> queryExpression, 
			DynamoDBMapperConfig config) {

		if(config == null) {
			config = DynamoDBMapperConfig.DEFAULT;
		}

		if(queryExpression == null) { //default query expression, no range key
			InvertedIndex item = new InvertedIndex();
			item.setWord(word);
			queryExpression = new DynamoDBQueryExpression<InvertedIndex>().withHashKeyValues(item);
		}


		PaginatedQueryList<InvertedIndex> collection 
		= DynamoTable.mapper.query(InvertedIndex.class, queryExpression, config);
		return collection;
	}

	/**
	 * load eagerly, slower but get all results readily available
	 * @param word
	 * @return
	 */
	public static PaginatedQueryList<InvertedIndex> queryEagerly(String word) {
		PaginatedQueryList<InvertedIndex> collection = query(word);
		collection.loadAllResults();
		return collection;
	}

	/**
	 * query but returns a list that can only use its iterator, it saves memory load
	 * and maybe faster
	 * @param word
	 * @return
	 */
	public static PaginatedQueryList<InvertedIndex> queryIterationOnly(String word) {
		DynamoDBMapperConfig config = new DynamoDBMapperConfig(
				DynamoDBMapperConfig.PaginationLoadingStrategy.ITERATION_ONLY);

		return query(word, null, config);
	}

	static final byte[][] spliter = new byte[17][];
	static {
		String zeros = "000000000000000000000000000000000000000"; //all F except for the left most character
		String sevfff = "7fffffffffffffffffffffffffffffffffffffff"; //all F except for the left most character
//		spliter[7] = new byte[40];
//		for(int i = 0; i < 40; i++) {
//			spliter[7][i] = 0;
//		}
		for(int i = 0; i < 16; i++) {
			String h = Integer.toHexString((i + 8) % 16);
			//			System.out.println(h + fff);
			spliter[i] = BinaryUtils.fromHex(h + zeros);
		}
		spliter[16] = BinaryUtils.fromHex(sevfff);
	}

	public static PaginatedQueryList<InvertedIndex> queryRange(String word, int index) {
		if(index < 0 || index > 15) {
			System.err.println("index number must be 0 ~ 15");
			throw new IllegalArgumentException();
		}
		InvertedIndex item = new InvertedIndex();
		item.setWord(word);

		byte[] start = spliter[index];
		byte[] end = null;
		if(index == 7) {
			System.out.println("7777");
			end = BinaryUtils.fromHex("ffffffffffffffffffffffffffffffffffffffff");
		} else if(index == 8) {
			System.out.println("8888");
			start = BinaryUtils.fromHex("ffffffffffffffffffffffffffffffffffffffff");
			end = BinaryUtils.fromHex("0fffffffffffffffffffffffffffffffffffffff");
		} else {
			System.out.println(index);
			end = spliter[index + 1];
		}
		Condition rangeKeyCondition = new Condition()
		.withComparisonOperator(ComparisonOperator.BETWEEN.toString())
		.withAttributeValueList(new AttributeValue().withB(ByteBuffer.wrap(start)), 
				new AttributeValue().withB(ByteBuffer.wrap(end)));

		DynamoDBQueryExpression<InvertedIndex> queryExpression 
		= new DynamoDBQueryExpression<InvertedIndex>().withHashKeyValues(item)
		.withRangeKeyCondition("id", rangeKeyCondition);


//		DynamoDBMapperConfig config = new DynamoDBMapperConfig(
//				DynamoDBMapperConfig.PaginationLoadingStrategy.ITERATION_ONLY);
		DynamoDBMapperConfig config = DynamoDBMapperConfig.DEFAULT;
		return query(word, queryExpression, config);
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

	public static void init () throws InterruptedException {
		createTable();
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
		createTable();
//		System.out.println("EFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF".length());
		
		//		System.out.println("EFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF".length());
		for(int i = 0; i < 16; i++) {
			System.out.println("===========" + i + "\t" + BinaryUtils.byteArrayToHexString(spliter[i]) + "\t" + BinaryUtils.byteArrayToHexString(spliter[i + 1]));			
			List<InvertedIndex> results = queryRange("kwanyama", i);
			Iterator<InvertedIndex> iterator = results.iterator();
			while(iterator.hasNext()) {
				System.out.println(BinaryUtils.byteArrayToHexString(iterator.next().id));
			}
		}
		
	}
}