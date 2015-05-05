package DynamoDB;

import java.nio.ByteBuffer;

import Utils.BinaryUtils;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
// TODO: Auto-generated Javadoc

/**
 * Object Persistent model, to populate docID-URL table .
 *
 * @author dichenli
 */
@DynamoDBTable(tableName="DocURL")
public class DocURL {
	
	/** The id. */
	byte[] id; //binary data
	
	/** The url. */
	String url;
	
	/**
	 * Gets the id.
	 *
	 * @return the id
	 */
	@DynamoDBHashKey(attributeName="id")
    public ByteBuffer getId() { return ByteBuffer.wrap(id); }
	
    /**
     * Sets the id.
     *
     * @param buf the new id
     */
    public void setId(ByteBuffer buf) { 
    	this.id = buf.array(); 
    }
    
    /**
     * Id set by string.
     *
     * @param decimalString the decimal string
     */
    public void idSetByString(String decimalString) {
    	id = BinaryUtils.fromDecimal(decimalString);
    }
    
    /**
     * Gets the url.
     *
     * @return the url
     */
    @DynamoDBAttribute(attributeName="url")
    public String getURL() { return url; }    
    
    /**
     * Sets the url.
     *
     * @param url the new url
     */
    public void setURL(String url) { this.url = url; }
    
    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
       return url;  
    }
    
    /**
     * Parses the input.
     *
     * @param line the line
     * @return the doc url
     */
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
    
    /**
     * Load from byte buffer.
     *
     * @param bytes the bytes
     * @return the doc url
     * @throws Exception the exception
     */
    public static DocURL loadFromByteBuffer(ByteBuffer bytes) throws Exception {
    	return load(bytes.array());
    }
    
    /**
     * Load from decimal string.
     *
     * @param decimalStr the decimal str
     * @return the doc url
     * @throws Exception the exception
     */
    public static DocURL loadFromDecimalString(String decimalStr) throws Exception {
    	return load(BinaryUtils.fromDecimal(decimalStr));
    }
    
    /**
     * Load.
     *
     * @param id the id
     * @return the doc url
     * @throws Exception the exception
     */
    public static DocURL load(byte[] id) throws Exception {
    	if (DynamoTable.mapper == null) {
    		DynamoTable.init();
    	}
    	return DynamoTable.mapper.load(DocURL.class, ByteBuffer.wrap(id));
    }
    
    /**
     * The main method.
     *
     * @param args the arguments
     * @throws Exception the exception
     */
    public static void main(String... args) throws Exception {
    	DynamoTable.init();
    	
    	System.out.println(loadFromDecimalString("478265070481920712437327189905938532370961602507"));
    	
    }

}
