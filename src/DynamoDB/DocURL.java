package DynamoDB;

import java.nio.ByteBuffer;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import Utils.BinaryUtils;
import com.sun.org.apache.bcel.internal.util.ByteSequence;
/**
 * Object Persistent model, to populate docID-URL table 
 * @author dichenli
 */
@DynamoDBTable(tableName="DocURL")
public class DocURL {
	
	byte[] id; //binary data
	String url;
	
	@DynamoDBHashKey(attributeName="id")
    public ByteBuffer getId() { return ByteBuffer.wrap(id); }
	
    public void setId(ByteBuffer buf) { 
    	this.id = buf.array(); 
    }
    
    public void setId(String hexString) {
    	id = BinaryUtils.fromHex(hexString);
    }
    
    @DynamoDBAttribute(attributeName="url")
    public String getTitle() { return url; }    
    public void setTitle(String url) { this.url = url; }
    
    @Override
    public String toString() {
       return url;  
    }

}
