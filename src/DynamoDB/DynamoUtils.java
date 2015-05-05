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
 * The Class DynamoUtils.
 */
public class DynamoUtils {


	/**
	 * Creates the table hash key.
	 *
	 * @param tableName the table name
	 * @param keyName the key name
	 * @param attrType the attr type
	 * @param readCapacity the read capacity
	 * @param writeCapacity the write capacity
	 * @return the creates the table request
	 */
	public static CreateTableRequest createTableHashKey(String tableName, String keyName, 
			ScalarAttributeType attrType, long readCapacity, long writeCapacity) {

		CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName);
		createTableRequest = setHashKey(createTableRequest, keyName, attrType);
		createTableRequest = setRWCapacity(createTableRequest, readCapacity, writeCapacity);
		return createTableRequest;
	}

	/**
	 * Sets the hash key.
	 *
	 * @param createTableRequest the create table request
	 * @param keyName the key name
	 * @param attrType the attr type
	 * @return the creates the table request
	 */
	static CreateTableRequest setHashKey(CreateTableRequest createTableRequest, 
			String keyName, ScalarAttributeType attrType) {
		return setKey(createTableRequest, keyName, attrType, KeyType.HASH);
	}

	/**
	 * Sets the range key.
	 *
	 * @param createTableRequest the create table request
	 * @param keyName the key name
	 * @param attrType the attr type
	 * @return the creates the table request
	 */
	static CreateTableRequest setRangeKey(CreateTableRequest createTableRequest, 
			String keyName, ScalarAttributeType attrType) {
		return setKey(createTableRequest, keyName, attrType, KeyType.RANGE);
	}

	/**
	 * Sets the key.
	 *
	 * @param createTableRequest the create table request
	 * @param keyName the key name
	 * @param attrType the attr type
	 * @param keyType the key type
	 * @return the creates the table request
	 */
	static CreateTableRequest setKey(CreateTableRequest createTableRequest, 
			String keyName, ScalarAttributeType attrType, KeyType keyType) {

		createTableRequest.withKeySchema(
				new KeySchemaElement().withAttributeName(keyName)
				.withKeyType(keyType));

		createTableRequest.withAttributeDefinitions(
				new AttributeDefinition().withAttributeName(keyName)
				.withAttributeType(attrType));

		return createTableRequest;
	}

	/**
	 * Sets the rw capacity.
	 *
	 * @param createTableRequest the create table request
	 * @param readCapacity the read capacity
	 * @param writeCapacity the write capacity
	 * @return the creates the table request
	 */
	static CreateTableRequest setRWCapacity(CreateTableRequest createTableRequest, 
			long readCapacity, long writeCapacity) {

		return createTableRequest.withProvisionedThroughput(
				new ProvisionedThroughput().withReadCapacityUnits(readCapacity)
				.withWriteCapacityUnits(writeCapacity));
	}

	/**
	 * Sets the key hash range.
	 *
	 * @param createTableRequest the create table request
	 * @param hashName the hash name
	 * @param hashType the hash type
	 * @param rangeName the range name
	 * @param rangeType the range type
	 * @return the creates the table request
	 */
	static CreateTableRequest setKeyHashRange(CreateTableRequest createTableRequest,
			String hashName, ScalarAttributeType hashType, 
			String rangeName, ScalarAttributeType rangeType) {

		//AttributeDefinitions
		ArrayList<AttributeDefinition> attributeDefinitions= new ArrayList<AttributeDefinition>();
		attributeDefinitions.add(new AttributeDefinition().withAttributeName(hashName).withAttributeType(hashType));
		attributeDefinitions.add(new AttributeDefinition().withAttributeName(rangeName).withAttributeType(rangeType));
		createTableRequest.setAttributeDefinitions(attributeDefinitions);
		
		//KeySchema
		ArrayList<KeySchemaElement> tableKeySchema = new ArrayList<KeySchemaElement>();
		tableKeySchema.add(new KeySchemaElement().withAttributeName(hashName).withKeyType(KeyType.HASH));
		tableKeySchema.add(new KeySchemaElement().withAttributeName(rangeName).withKeyType(KeyType.RANGE));
		createTableRequest.setKeySchema(tableKeySchema);
		return createTableRequest;
	}

	/**
	 * Creates the table hash range.
	 *
	 * @param tableName the table name
	 * @param hashName the hash name
	 * @param hashType the hash type
	 * @param rangeName the range name
	 * @param rangeType the range type
	 * @param readCapacity the read capacity
	 * @param writeCapacity the write capacity
	 * @return the creates the table request
	 */
	public static CreateTableRequest createTableHashRange(String tableName, 
			String hashName, ScalarAttributeType hashType, 
			String rangeName, ScalarAttributeType rangeType,
			long readCapacity, long writeCapacity) {
		
		CreateTableRequest createTableRequest = new CreateTableRequest()
		.withTableName(tableName);
		createTableRequest = setKeyHashRange(createTableRequest, hashName, 
				hashType, rangeName, rangeType);
		createTableRequest = setRWCapacity(createTableRequest, readCapacity, writeCapacity);
		return createTableRequest;
	}
	

	
	
}
