/**
 * 
 */
package DynamoDB;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import Utils.BinaryUtils;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedQueryList;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

// TODO: Auto-generated Javadoc
/**
 * The Class QueryRecord.
 *
 * @author dichenli
 * Record query data to optimize query results
 */
@DynamoDBTable(tableName="QueryRecord")
public class QueryRecord {
	
	/** The table name. */
	static String tableName = "QueryRecord";
	
	/** The hash key. */
	static String hashKey = "query";
	
	/** The range key. */
	static String rangeKey = "id";
	
	/** The read capacity. */
	static long readCapacity = 25L;
	
	/** The write capacity. */
	static long writeCapacity = 25L;
	
	/** The inserter. */
	static Inserter<QueryRecord> inserter = new Inserter<QueryRecord>();

	/** The query. */
	String query; //many words
	
	/** The id. */
	byte[] id; //docID
	
	/** The count. */
	int count;
	
	/**
	 * Instantiates a new query record.
	 */
	public QueryRecord() {}
	
	/**
	 * Instantiates a new query record.
	 *
	 * @param query the query
	 */
	public QueryRecord(String query) {
		this.query = query;
	}
	
	/**
	 * Instantiates a new query record.
	 *
	 * @param query the query
	 * @param docID the doc id
	 */
	public QueryRecord(String query, ByteBuffer docID) {
		this.query = query;
		this.id = docID.array();
	}
 	
	/**
	 * Instantiates a new query record.
	 *
	 * @param query the query
	 * @param decimalID the decimal id
	 */
	public QueryRecord(String query, String decimalID) {
		this.query = query;
		this.id = BinaryUtils.fromDecimal(decimalID);
		this.count = 0;
	}
	
	/**
	 * Instantiates a new query record.
	 *
	 * @param query the query
	 * @param decimalID the decimal id
	 * @param count the count
	 */
	public QueryRecord(String query, String decimalID, int count) {
		this.query = query;
		this.id = BinaryUtils.fromDecimal(decimalID);
		this.count = count;
	} 
	
	
	/**
	 * Gets the query.
	 *
	 * @return the query
	 */
	@DynamoDBHashKey(attributeName="query")
	public String getQuery() {
		return query;
	}
	
	/**
	 * Sets the query.
	 *
	 * @param query the new query
	 */
	public void setQuery(String query) {
		this.query = query;
	}

	/**
	 * Gets the id.
	 *
	 * @return the id
	 */
	@DynamoDBRangeKey(attributeName="id")
	public ByteBuffer getId() {
		return ByteBuffer.wrap(id);
	}
	
	/**
	 * Sets the id.
	 *
	 * @param id the new id
	 */
	public void setId(ByteBuffer id) {
		this.id = id.array();
	}

	/**
	 * Gets the count.
	 *
	 * @return the count
	 */
	@DynamoDBAttribute(attributeName="count")
	public int getCount() {
		return count;
	}
	
	/**
	 * Sets the count.
	 *
	 * @param count the new count
	 */
	public void setCount(int count) {
		this.count = count;
	}
	
	

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return query +"\t" + BinaryUtils.byteArrayToDecimalString(id) + "\t" + count;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object other) {
		if(other == null || !(other instanceof QueryRecord)) {
			return false;
		}

		QueryRecord other2 = (QueryRecord) other;
		if(!other2.query.equals(this.query)) {
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
		return query.hashCode() * 31 + Arrays.hashCode(id);
	}
	
	
	/**
	 * Generate a table. Must call this method before any other static methods
	 * can be used
	 *
	 * @throws InterruptedException the interrupted exception
	 */
	public static void createTable() throws InterruptedException {
		CreateTableRequest request = DynamoUtils.createTableHashRange(
				tableName, hashKey, ScalarAttributeType.S, 
				rangeKey, ScalarAttributeType.B, 
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
		inserter = new Inserter<QueryRecord>();
	}
	
	/**
	 * Insert.
	 *
	 * @param item the item
	 */
	public static void insert(QueryRecord item) {
		inserter.insert(item, true);
	}
	
	/**
	 * insert an record with query words, decimal representation of SHA-1 ID,
	 * and set count to 0.
	 *
	 * @param query the query
	 * @param decimalID the decimal id
	 */
	public static void insert(String query, String decimalID) {
		insert(new QueryRecord(query, decimalID, 1));
	}
	
	/**
	 * insert an record with query words, decimal representation of SHA-1 ID,
	 * and set count to the given count.
	 *
	 * @param query the query
	 * @param decimalID the decimal id
	 * @param count the count
	 */
	public static void insert(String query, String decimalID, int count) {
		insert(new QueryRecord(query, decimalID, count));
	}
	
	/**
	 * find match results from the given query words.
	 *
	 * @param query the query
	 * @return a java.util.List of matching results
	 */
	public static PaginatedQueryList<QueryRecord> find(String query) {
		QueryRecord item = new QueryRecord();
		item.setQuery(query);
		DynamoDBQueryExpression<QueryRecord> queryExpression 
		= new DynamoDBQueryExpression<QueryRecord>().withHashKeyValues(item);
		
		PaginatedQueryList<QueryRecord> collection 
		= DynamoTable.mapper.query(QueryRecord.class, queryExpression);
		return collection;
	}
	
	/**
	 * precisely match a given result from given query and docID. returns null
	 * if no match
	 *
	 * @param query the query
	 * @param decimalID the decimal id
	 * @return the query record
	 */
	public static QueryRecord load(String query, String decimalID) {
		QueryRecord item = new QueryRecord(query, decimalID);
		return DynamoTable.mapper.load(item);
	}
	
	/**
	 * Load.
	 *
	 * @param query the query
	 * @param docID the doc id
	 * @return the query record
	 */
	public static QueryRecord load(String query, ByteBuffer docID) {
		QueryRecord item = new QueryRecord(query, docID);
		return DynamoTable.mapper.load(item);
	}
	
	/**
	 * increment the count of the record by 1. It calls load and insert, sends
	 * two remote function calls, so this function call is expensive
	 *
	 * @param query the query
	 * @param decimalID the decimal id
	 */
	public static void increment(String query, String decimalID) {
		QueryRecord item = load(query, decimalID);
		if(item == null) insert(query, decimalID);
		else{
			item.count++;
			insert(item);
		}
	}
	
	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws InterruptedException the interrupted exception
	 */
	public static void main(String[] args) throws InterruptedException {
		createTable();
		insert("test", "1024");
		insert("test", "2048", 2);
		increment("test", "2048");
		System.out.println(load("test", "2048"));
		List<QueryRecord> results = find("test");
		for(QueryRecord q : results) {
			System.out.println(q);
		}
	}

}
