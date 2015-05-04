package DynamoDB;

import java.nio.ByteBuffer;

import Utils.BinaryUtils;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
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
    
    public void idSetByString(String decimalString) {
    	id = BinaryUtils.fromDecimal(decimalString);
    }
    
    @DynamoDBAttribute(attributeName="url")
    public String getURL() { return url; }    
    public void setURL(String url) { this.url = url; }
    
    @Override
    public String toString() {
       return url;  
    }
    
    public static DocURL parseInput(String line) {
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
			System.out.println("empty content: " + line);
			return null;
		}
		
		DocURL item = new DocURL();
		item.idSetByString(docID);
		item.setURL(url);
		return item;
	}
    
    public static DocURL loadFromByteBuffer(ByteBuffer bytes) throws Exception {
    	return load(bytes.array());
    }
    
    public static DocURL loadFromDecimalString(String decimalStr) throws Exception {
    	return load(BinaryUtils.fromDecimal(decimalStr));
    }
    
    public static DocURL load(byte[] id) throws Exception {
    	if (DynamoTable.mapper == null) {
    		DynamoTable.init();
    	}
    	return DynamoTable.mapper.load(DocURL.class, ByteBuffer.wrap(id));
    }
    
    public static void main(String... args) throws Exception {
    	DynamoTable.init();
    	
    	System.out.println(loadFromDecimalString("478265070481920712437327189905938532370961602507"));
    	
    }

}
