package DynamoDB;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

// TODO: Auto-generated Javadoc
/**
 * for experiment only, populate a string-string database table.
 *
 * @author dichenli
 */
@DynamoDBTable(tableName="StrStr")
public class StrStr {

	
	/** The id. */
	String id; //binary data
	
	/** The url. */
	String url;
	
	/**
	 * Gets the id.
	 *
	 * @return the id
	 */
	@DynamoDBHashKey(attributeName="id")
    public String getId() { return id; }
    
    /**
     * Sets the id.
     *
     * @param id the new id
     */
    public void setId(String id) { this.id = id; }
    
    /**
     * Gets the title.
     *
     * @return the title
     */
    @DynamoDBAttribute(attributeName="url")
    public String getTitle() { return url; }    
    
    /**
     * Sets the title.
     *
     * @param url the new title
     */
    public void setTitle(String url) { this.url = url; }
    
    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
       return url;            
    }


}
