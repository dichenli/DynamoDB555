/**
 * 
 */
package DynamoDB;

import java.io.File;
import java.util.Date;
import java.util.ArrayList;
import java.util.HashSet;
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
 * The Class InvertedIndexPopulator.
 *
 * @author dichenli
 * No longer used, used for old sample data
 */
@Deprecated
public class InvertedIndexPopulator implements Populator {
	
	/** The table name. */
	static String tableName = NewInvertedIndex.tableName; //need to sync with @DynamoDBTable(tableName="xx")
//	static String word = "word";
//	static String id = "id";
	/** The hash key. */
static String hashKey = NewInvertedIndex.hashKey;
	
	/** The range key. */
	static String rangeKey = NewInvertedIndex.rangeKey;
	
	/** The read capacity. */
	static long readCapacity = NewInvertedIndex.readCapacity;
	
	/** The write capacity. */
	static long writeCapacity = NewInvertedIndex.writeCapacity;
	
	/** The input. */
	File input;

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
		CreateTableRequest createTableRequest = new CreateTableRequest()
		.withTableName(tableName)
		.withProvisionedThroughput(
				new ProvisionedThroughput()
				.withReadCapacityUnits(readCapacity)
				.withWriteCapacityUnits(writeCapacity));
		
		//AttributeDefinitions
		ArrayList<AttributeDefinition> attributeDefinitions= new ArrayList<AttributeDefinition>();
		attributeDefinitions.add(new AttributeDefinition().withAttributeName(hashKey).withAttributeType(ScalarAttributeType.S));
		attributeDefinitions.add(new AttributeDefinition().withAttributeName(rangeKey).withAttributeType(ScalarAttributeType.B));
		createTableRequest.setAttributeDefinitions(attributeDefinitions);
		
		//KeySchema
		ArrayList<KeySchemaElement> tableKeySchema = new ArrayList<KeySchemaElement>();
		tableKeySchema.add(new KeySchemaElement().withAttributeName(hashKey).withKeyType(KeyType.HASH));
		tableKeySchema.add(new KeySchemaElement().withAttributeName(rangeKey).withKeyType(KeyType.RANGE));
		createTableRequest.setKeySchema(tableKeySchema);
		
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
		
//		ArrayList<InvertedIndex> items = new ArrayList<InvertedIndex>();
		if(sc == null) {
			throw new NullPointerException();
		}
		while(sc.hasNextLine()) {
			NewInvertedIndex item = NewInvertedIndex.parseInput(sc.nextLine());
			NewInvertedIndex.insert(item, false);
			current++;
			
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
		System.out.println("done");
		sc.close();
	}
	
	/**
	 * Instantiates a new inverted index populator.
	 *
	 * @param fileName the file name
	 */
	public InvertedIndexPopulator(String fileName) {
		this.input = new File(fileName);
		if(input == null) {
			throw new IllegalArgumentException();
		}
		if(!Utils.IOUtils.fileExists(input)) {
			throw new IllegalArgumentException();
		}
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
		Populator instance = new InvertedIndexPopulator(input);
		instance.createTable();
		instance.populate();
	}
}
