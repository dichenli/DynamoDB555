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

public class DynamoUtils {


	public static CreateTableRequest createTableHashKey(String tableName, String keyName, 
			ScalarAttributeType attrType, long readCapacity, long writeCapacity) {

		CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName);
		createTableRequest = setHashKey(createTableRequest, keyName, attrType);
		createTableRequest = setRWCapacity(createTableRequest, readCapacity, writeCapacity);
		return createTableRequest;
	}

	static CreateTableRequest setHashKey(CreateTableRequest createTableRequest, 
			String keyName, ScalarAttributeType attrType) {
		return setKey(createTableRequest, keyName, attrType, KeyType.HASH);
	}

	static CreateTableRequest setRangeKey(CreateTableRequest createTableRequest, 
			String keyName, ScalarAttributeType attrType) {
		return setKey(createTableRequest, keyName, attrType, KeyType.RANGE);
	}

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

	static CreateTableRequest setRWCapacity(CreateTableRequest createTableRequest, 
			long readCapacity, long writeCapacity) {

		return createTableRequest.withProvisionedThroughput(
				new ProvisionedThroughput().withReadCapacityUnits(readCapacity)
				.withWriteCapacityUnits(writeCapacity));
	}

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
