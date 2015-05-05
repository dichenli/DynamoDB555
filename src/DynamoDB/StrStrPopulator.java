package DynamoDB;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import Utils.IOUtils;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
// TODO: Auto-generated Javadoc

/**
 * for experiment only, populate a string-string database table.
 *
 * @author dichenli
 */
public class StrStrPopulator implements Populator {
	
	/** The table name. */
	static String tableName = "StrStr"; //need to sync with @DynamoDBTable(tableName="XXX")
	
	/** The key name. */
	static String keyName = "id";
	
	/** The read capacity. */
	static long readCapacity = 1L;
	
	/** The write capacity. */
	static long writeCapacity = 1L;
	
	/** The input. */
	File input;
	
	/** The table. */
	DynamoTable table;
	
	/** The sc. */
	Scanner sc;
	
	/**
	 * Instantiates a new str str populator.
	 *
	 * @param fileName the file name
	 * @throws Exception the exception
	 */
	public StrStrPopulator(String fileName) throws Exception {
		this(new File(fileName));
	}
	
	/**
	 * Instantiates a new str str populator.
	 *
	 * @param input the input
	 * @throws Exception the exception
	 */
	public StrStrPopulator(File input) throws Exception {
		if(input == null) {
			throw new IllegalArgumentException();
		}
		if(!Utils.IOUtils.fileExists(input)) {
			throw new IllegalArgumentException();
		}
		sc = IOUtils.getScanner(input);
		if(sc == null) {
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
		while(sc.hasNextLine()) {
			StrStr item = parseStrStr(sc.nextLine());
			if(item != null) {
				DynamoTable.insert(item);	
			}
		}
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
				.withAttributeDefinitions(new AttributeDefinition().withAttributeName(keyName).withAttributeType(ScalarAttributeType.S))
				.withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(readCapacity).withWriteCapacityUnits(writeCapacity));
		return createTableRequest;
	}
	
	
	/**
	 * Parses the str str.
	 *
	 * @param line the line
	 * @return the str str
	 */
	private StrStr parseStrStr(String line) {
		if(line == null) {
			System.out.println("null line");
			return null;
		}
		
		String[] splited = line.split("\t");
		if(splited == null || splited.length != 2) {
			System.out.println("bad line: " + line);
			return null;
		}
		String docID = splited[0];
		String url = splited[1];
		if(docID.equals("") || url.equals("")) {
			System.out.println("bad line: " + line);
			return null;
		}
		
		StrStr item = new StrStr();
		item.setId(docID);
		item.setTitle(url);
		return item;
	}
	
	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws Exception the exception
	 */
	public static void main(String[] args) throws Exception {
		StrStrPopulator instance = new StrStrPopulator("/Users/dichenli/Documents/course materials/eclipse/DynamoDB555/id-1.txt");
		instance.createTable();
		instance.populate();
	}

	

	

	

}
