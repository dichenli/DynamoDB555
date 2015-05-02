/**
 * 
 */
package DynamoDB;

/**
 * @author dichenli
 * an abstract item
 */
public abstract class Item {
	public Item() {}
	
	 public Item(String line) {
		 parse(line);
	 }
	 
	 public abstract void parse(String line);
}
