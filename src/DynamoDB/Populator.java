package DynamoDB;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;

// TODO: Auto-generated Javadoc
/**
 * The Interface Populator.
 */
public interface Populator {

	/**
	 * get table name, must be the same as the table name on @DynamoDBTable(tableName="XXX").
	 *
	 * @return the table name
	 */
	String getTableName();
	
	/**
	 * set table specs, like key name, key type, etc.
	 *
	 * @return the creates the table request
	 */
	CreateTableRequest createTableRequest();
	
	/**
	 * call DynamoTable.createTable(this) to instantiate the table 
	 *
	 * @throws Exception the exception
	 */
	void createTable() throws Exception;
	
	/**
	 * execute code to populate the database. 
	 */
	void populate();
}
