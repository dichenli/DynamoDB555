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

/**
 * @author dichenli
 * Record query data to optimize query results
 */
@DynamoDBTable(tableName="QueryRecord")
public class QueryRecord {
	
	static String tableName = "QueryRecord";
	static String hashKey = "query";
	static String rangeKey = "id";
	static long readCapacity = 25L;
	static long writeCapacity = 25L;
	
	static Inserter<QueryRecord> inserter = new Inserter<QueryRecord>();

	String query; //many words
	byte[] id; //docID
	int count;
	
	public QueryRecord() {}
	
	public QueryRecord(String query) {
		this.query = query;
	}
	
	public QueryRecord(String query, ByteBuffer docID) {
		this.query = query;
		this.id = docID.array();
	}
 	
	public QueryRecord(String query, String decimalID) {
		this.query = query;
		this.id = BinaryUtils.fromDecimal(decimalID);
		this.count = 0;
	}
	
	public QueryRecord(String query, String decimalID, int count) {
		this.query = query;
		this.id = BinaryUtils.fromDecimal(decimalID);
		this.count = count;
	} 
	
	
	/**
	 * @return the query
	 */
	@DynamoDBHashKey(attributeName="query")
	public String getQuery() {
		return query;
	}
	public void setQuery(String query) {
		this.query = query;
	}

	/**
	 * @return the id
	 */
	@DynamoDBRangeKey(attributeName="id")
	public ByteBuffer getId() {
		return ByteBuffer.wrap(id);
	}
	public void setId(ByteBuffer id) {
		this.id = id.array();
	}

	/**
	 * @return the count
	 */
	@DynamoDBAttribute(attributeName="count")
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	
	

	@Override
	public String toString() {
		return query +"\t" + BinaryUtils.byteArrayToString(id) + "\t" + count;
	}
	
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

	@Override
	public int hashCode() {
		return query.hashCode() * 31 + Arrays.hashCode(id);
	}
	
	
	/**
	 * Generate a table. Must call this method before any other static methods
	 * can be used
	 * @throws InterruptedException
	 */
	public static void createTable() throws InterruptedException {
		CreateTableRequest request = DynamoUtils.createTableHashRange(
				tableName, hashKey, ScalarAttributeType.S, 
				rangeKey, ScalarAttributeType.B, 
				readCapacity, writeCapacity);

		DynamoTable.createTable(tableName, request);
	}
	
	public static void insert(QueryRecord item) {
		inserter.insert(item, true);
	}
	
	/**
	 * insert an record with query words, decimal representation of SHA-1 ID,
	 * and set count to 0
	 * @param query
	 * @param decimalID
	 */
	public static void insert(String query, String decimalID) {
		insert(new QueryRecord(query, decimalID, 1));
	}
	
	/**
	 * insert an record with query words, decimal representation of SHA-1 ID,
	 * and set count to the given count
	 * @param query
	 * @param decimalID
	 * @param count
	 */
	public static void insert(String query, String decimalID, int count) {
		insert(new QueryRecord(query, decimalID, count));
	}
	
	/**
	 * find match results from the given query words
	 * @param query
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
	 * @param query
	 * @param decimalID
	 * @return
	 */
	public static QueryRecord load(String query, String decimalID) {
		QueryRecord item = new QueryRecord(query, decimalID);
		return DynamoTable.mapper.load(item);
	}
	
	public static QueryRecord load(String query, ByteBuffer docID) {
		QueryRecord item = new QueryRecord(query, docID);
		return DynamoTable.mapper.load(item);
	}
	
	/**
	 * increment the count of the record by 1. It calls load and insert, sends
	 * two remote function calls, so this function call is expensive
	 * @param query
	 * @param decimalID
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
	 * @param args
	 * @throws InterruptedException 
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
