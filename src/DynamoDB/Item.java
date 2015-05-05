/**
 * 
 */
package DynamoDB;

// TODO: Auto-generated Javadoc
/**
 * The Class Item.
 *
 * @author dichenli
 * an abstract item
 */
public abstract class Item {
	
	/**
	 * Instantiates a new item.
	 */
	public Item() {}
	
	 /**
 	 * Instantiates a new item.
 	 *
 	 * @param line the line
 	 */
 	public Item(String line) {
		 parse(line);
	 }
	 
	 /**
 	 * Parses the.
 	 *
 	 * @param line the line
 	 */
 	public abstract void parse(String line);
}
