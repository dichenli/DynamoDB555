/**
 * 
 */
package DynamoDB;

import java.nio.ByteBuffer;
import java.util.Arrays;

import Utils.BinaryUtils;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

/**
 * @author dichenli
 * data of page rank
 */
@DynamoDBTable(tableName="PageRank")
public class PageRank {
	byte[] id; //binary data
	float rank; //page rank
	
	@DynamoDBHashKey(attributeName="id")
    public ByteBuffer getId() { return ByteBuffer.wrap(id); }
	
    public void setId(ByteBuffer buf) { 
    	this.id = buf.array(); 
    }
    
    public void setId(String hexString) {
    	id = BinaryUtils.fromHex(hexString);
    }
    
    @DynamoDBAttribute(attributeName="rank")
    public float getRank() { return rank; }    
    public void setRank(float rank) { this.rank = rank; }
    
    @Override
    public String toString() {
       return Arrays.toString(id);  
    }
	
}
