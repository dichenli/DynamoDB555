/**
 * 
 */
package DynamoDB;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import Utils.*;

import javax.swing.text.TabExpander;

import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

import Utils.nameUtils;


// TODO: Auto-generated Javadoc
/**
 * The Class DocURLPopulator.
 *
 * @author dichenli
 */
public class DocURLPopulator implements Populator {
	
	/** The table name. */
	static String tableName = "DocURL"; //need to sync with @DynamoDBTable(tableName="xx")
	
	/** The key name. */
	static String keyName = "id";
	
	/** The read capacity. */
	static long readCapacity = 1L;
	
	/** The write capacity. */
	static long writeCapacity = 1000L;
	
	/** The input. */
	File input;
	
	/** The sc. */
	Scanner sc;
	
	/**
	 * Instantiates a new doc url populator.
	 *
	 * @param fileName the file name
	 * @throws Exception the exception
	 */
	public DocURLPopulator(String fileName) throws Exception {
		this(new File(fileName));
	}
	
	
	/**
	 * Instantiates a new doc url populator.
	 *
	 * @param input the input
	 * @throws Exception the exception
	 */
	public DocURLPopulator(File input) throws Exception {
		this.input = input;
		if(input == null) {
			throw new IllegalArgumentException();
		}
		if(!Utils.IOUtils.fileExists(input)) {
			throw new IllegalArgumentException();
		}
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
		
		ArrayList items = new ArrayList();
		if(sc == null) {
			throw new NullPointerException();
		}
		while(sc.hasNextLine()) {
			DocURL item = DocURL.parseInput(sc.nextLine());
			if(item != null) {
				items.add(item);
				if(items.size() >= 25) {
					failed += DynamoTable.batchInsert(items);
					count += items.size();
					current += items.size();
					items = new ArrayList<DocURL>();
				}
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
	
	/* (non-Javadoc)
	 * @see DynamoDB.Populator#getTableName()
	 */
	@Override
	public String getTableName() {
		return tableName;
	}
	
	/**
	 * Create a table with "Hash" key type and String key as the table key.
	 *
	 * @return the creates the table request
	 */
	@Override
	public CreateTableRequest createTableRequest() {
		CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
				.withKeySchema(new KeySchemaElement().withAttributeName(keyName).withKeyType(KeyType.HASH))
				.withAttributeDefinitions(new AttributeDefinition().withAttributeName(keyName).withAttributeType(ScalarAttributeType.B))
				.withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(readCapacity).withWriteCapacityUnits(writeCapacity));
		return createTableRequest;
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
		Populator instance = new DocURLPopulator(input);
		instance.createTable();
		instance.populate();
	}
	
}
