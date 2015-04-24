package DynamoDB;

import java.io.File;
import java.util.Scanner;

import Utils.IOUtils;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

public class PageRankPopulator implements Populator {

	static String tableName = "PageRank"; //need to sync with @DynamoDBTable(tableName="xx")
	static String keyName = "id";
	static long readCapacity = 2L; // 10 at most. Or we will be charged
	static long writeCapacity = 2L; // 10 at most. Or we will be charged
	
	
	File input;
	DynamoTable table;
	Scanner sc;
	
	public PageRankPopulator(String fileName) {
		this(new File(fileName));
	}
	
	public PageRankPopulator(File input) {
		this.input = input;
		if(input == null) {
			throw new IllegalArgumentException();
		}
		if(!Utils.IOUtils.fileExists(input)) {
			throw new IllegalArgumentException();
		}
	}
	
	@Override
	public String getTableName() {
		return tableName;
	}

	@Override
	public CreateTableRequest createTableRequest() {
		CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
				.withKeySchema(new KeySchemaElement().withAttributeName(keyName).withKeyType(KeyType.HASH))
				.withAttributeDefinitions(new AttributeDefinition().withAttributeName(keyName).withAttributeType(ScalarAttributeType.B))
				.withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(readCapacity).withWriteCapacityUnits(writeCapacity));
		return createTableRequest;
	}

	@Override
	public void createTable() throws Exception {
		DynamoTable.creatTable(this);
	}

	@Override
	public void populate() {
		sc = IOUtils.getScanner(input);
		if(sc == null) {
			throw new NullPointerException();
		}
		while(sc.hasNextLine()) {
			PageRank item = parsePageRank(sc.nextLine());
			if(item != null) {
				DynamoTable.insert(item);	
			}
		}
		sc.close();
		
	}
	
	private PageRank parsePageRank(String line) {
		if(line == null) {
			System.out.println("null line");
			return null;
		}
		
		String[] splited = line.split("\t");
		if(splited == null || splited.length != 2) {
			System.out.println("bad line: " + line);
			return null;
		}
		String docID = splited[1];
		float rank;
		try {
			rank = Float.parseFloat(splited[0]);
		} catch(Exception e) {
			e.printStackTrace();
			return null;
		}
		
		if(docID.equals("")) {
			System.out.println("bad line: " + line);
			return null;
		}
		
		PageRank item = new PageRank();
		item.setId(docID);
		item.setRank(rank);
		return item;
	}
	
	public static void main(String[] args) throws Exception {
		PageRankPopulator instance = new PageRankPopulator("/Users/dichenli/Documents/course materials/eclipse/DynamoDB555/pagerank_sample.txt");
		instance.createTable();
		instance.populate();
	}
	
}
