package DynamoDB;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.Scanner;

import Utils.IOUtils;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

// TODO: Auto-generated Javadoc
/**
 * The Class PageRankPopulator.
 */
public class PageRankPopulator implements Populator {

	/** The table name. */
	static String tableName = PageRank.tableName; //need to sync with @DynamoDBTable(tableName="xx")
	
	/** The key name. */
	static String keyName = PageRank.keyName;
	
	/** The read capacity. */
	static long readCapacity = PageRank.readCapacity; // 10 at most. Or we will be charged
	
	/** The write capacity. */
	static long writeCapacity = PageRank.writeCapacity; // 10 at most. Or we will be charged
	
	/** The input. */
	File input;
	
	/** The sc. */
	Scanner sc;
	
	/**
	 * Instantiates a new page rank populator.
	 *
	 * @param fileName the file name
	 */
	public PageRankPopulator(String fileName) {
		this(new File(fileName));
	}
	
	/**
	 * Instantiates a new page rank populator.
	 *
	 * @param input the input
	 */
	public PageRankPopulator(File input) {
		this.input = input;
		if(input == null) {
			throw new IllegalArgumentException();
		}
		if(!Utils.IOUtils.fileExists(input)) {
			throw new IllegalArgumentException();
		}
	}
	
	/* (non-Javadoc)
	 * @see DynamoDB.Populator#getTableName()
	 */
	@Override
	public String getTableName() {
		return tableName;
	}

	/* (non-Javadoc)
	 * @see DynamoDB.Populator#createTableRequest()
	 */
	@Override
	public CreateTableRequest createTableRequest() {
		CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
				.withKeySchema(new KeySchemaElement().withAttributeName(keyName).withKeyType(KeyType.HASH))
				.withAttributeDefinitions(new AttributeDefinition().withAttributeName(keyName).withAttributeType(ScalarAttributeType.B))
				.withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(readCapacity).withWriteCapacityUnits(writeCapacity));
		return createTableRequest;
	}

	/* (non-Javadoc)
	 * @see DynamoDB.Populator#createTable()
	 */
	@Override
	public void createTable() throws Exception {
		DynamoTable.creatTable(this);
	}
	
	/* (non-Javadoc)
	 * @see DynamoDB.Populator#populate()
	 */
	@Override
	public void populate() {
		long total = IOUtils.countLines(input); 
		Scanner sc = IOUtils.getScanner(input);
		long count = 0;
		long current = 0;
		long begin = new Date().getTime();
		long last = begin;
		long failed = 0;
		
		ArrayList<PageRank> items = new ArrayList<PageRank>();
		if(sc == null) {
			throw new NullPointerException();
		}
		while(sc.hasNextLine()) {
			try {
				PageRank item = new PageRank(sc.nextLine());
				items.add(item);
				if(items.size() >= 25) {
					failed += DynamoTable.batchInsert(items);
					count += items.size();
					current += items.size();
					items = new ArrayList<PageRank>();
				}
			} catch (Exception e) {
				e.printStackTrace();
				continue;
			}
			
			if(current >= 500) {
				long now = new Date().getTime();
				float time1 = (float)(now - begin) ;
				float time = (float)(now - last) ;
				if(time < 1) {
					time = 1;
				}
				if(time1 < 1) {
					time1 = 1;
				}
				System.out.println("======" + count + ", total speed:" 
				+ (((float)count / time1) *1000) + "item/sec, current speed: " 
				+ (((float)current / time) *1000) + "item/sec, " 
				+ ((float)count /(float) total) + "%, failed: "
				+ failed +"======");
				current = 0;
				last = now;
			}
		}
		if(!items.isEmpty()) {
			failed += DynamoTable.batchInsert(items);
		}
		System.out.println("done, count: " + count + ", failed: " + failed);
		sc.close();
	}
	
	
	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws Exception the exception
	 */
	public static void main(String[] args) throws Exception {
		if(args.length != 1 || args[0].equals("")) {
			System.out.println("Usage: <jar_name> <input_file>");
		}
		String input = args[0];
		Populator instance = new PageRankPopulator(input);
		instance.createTable();
		instance.populate();
	}
	
}
