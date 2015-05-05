/**
 * 
 */
package DynamoDB;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import Utils.BinaryUtils;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

// TODO: Auto-generated Javadoc
/**
 * The Class Anchor.
 *
 * @author dichenli
 */
@DynamoDBTable(tableName="Anchor")
public class Anchor {
	
	/** The id. */
	byte[] id; //binary data, docID
	
	/** The word. */
	String word; 
	
	/** The types. */
	HashSet<Integer> types; //position of the word in document


	/**
	 * Instantiates a new anchor.
	 *
	 * @param word2 the word2
	 * @param id2 the id2
	 * @param types the types
	 */
	public Anchor(String word2, byte[] id2, HashSet<Integer> types) {
		this.word = word2;
		this.id = id2;
		this.types = types;
	}

	/**
	 * Instantiates a new anchor.
	 */
	public Anchor() {
		types = new HashSet<Integer>();
		this.id = new byte[20];
	}

	/**
	 * Gets the id.
	 *
	 * @return the id
	 */
	@DynamoDBRangeKey(attributeName="id")
	public ByteBuffer getId() { return ByteBuffer.wrap(id); }

	/**
	 * Sets the id.
	 *
	 * @param buf the new id
	 */
	public void setId(ByteBuffer buf) {
		if(buf == null) {
			System.err.println("ByteBuffer null");
			return;
		}
		id = buf.array();
	}

	/**
	 * Sets the id.
	 *
	 * @param hexString the new id
	 */
	public void setId(String hexString) {
		id = BinaryUtils.fromDecimal(hexString);
	}

	/**
	 * Gets the word.
	 *
	 * @return the word
	 */
	@DynamoDBHashKey(attributeName="word")
	public String getWord() { return word; }    

	/**
	 * Sets the word.
	 *
	 * @param word the new word
	 */
	public void setWord(String word) { this.word = word; }

	/**
	 * Gets the types.
	 *
	 * @return the types
	 */
	@DynamoDBAttribute(attributeName="types")
	public Set<Integer> getTypes() {
		return types;
	}
	
	/**
	 * Sets the types.
	 *
	 * @param types the new types
	 */
	public void setTypes(Set<Integer> types) {
		this.types.addAll(types);
	}

	/**
	 * Adds the type.
	 *
	 * @param type the type
	 */
	public void addType(Integer type) {
		types.add(type);
	}


	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return word + BinaryUtils.byteArrayToDecimalString(id);
	}

	/**
	 * Parses the input.
	 *
	 * @param line the line
	 * @return the anchor
	 */
	public static Anchor parseInput(String line) {
		if (line == null) {
			System.err.println("parseInput: null line!");
			return null;
		}

		String[] splited = line.split("\t");
		if (splited.length != 3) {
			System.err.println("parseInput: bad line: " + line);
			return null;
		}

		String word = splited[0].trim();
		if (word.equals("")) {
			System.err.println("parseInput: word empty: " + line);
			return null;
		}

		byte[] id = BinaryUtils.fromDecimal(splited[1].trim());
		if (id.length == 0) {
			System.err.println("parseInput: id wrong: " + line);
			return null;
		}

		String[] typesStrs = splited[2].split(",");
		if (typesStrs.length == 0) {
			System.err.println("parseInput: positions wrong: " + line);
			return null;
		}

		HashSet<Integer> types = new HashSet<Integer>();
		for (String p : typesStrs) {
			try {
				Integer pos = Integer.parseInt(p);
				types.add(pos);
			} catch(Exception e) {
				System.err.println("parseInput: positions wrong: " + line);
				return null;
			}
		}

		return new Anchor(word, id, types);
	}

	/**
	 * Load.
	 *
	 * @param word the word
	 * @param id the id
	 * @return the anchor
	 * @throws Exception the exception
	 */
	public static Anchor load(String word, ByteBuffer id) throws Exception {
		if (DynamoTable.mapper == null) {
			DynamoTable.init();
		}
		return DynamoTable.mapper.load(DynamoDB.Anchor.class, word, id);
	}

	/**
	 * Query page.
	 *
	 * @param queryWord the query word
	 * @return the list
	 */
	public static List<Anchor> queryPage(String queryWord) {
		if (DynamoTable.mapper == null) {
			try {
				DynamoTable.init();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		Anchor wordKey = new Anchor();
		wordKey.setWord(queryWord);
		DynamoDBQueryExpression<Anchor> queryExpression = new DynamoDBQueryExpression<Anchor>().withHashKeyValues(wordKey);

		List<Anchor> collection = DynamoTable.mapper.queryPage(Anchor.class, queryExpression).getResults();
		return collection;
	}

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws Exception the exception
	 */
	public static void main(String[] args) throws Exception {
		for(int i = 0; i < 1; i++) {
			List<Anchor> results = queryPage("the");
			for(Anchor a : results) {
				System.out.println(a);
			}
			Anchor a = results.remove(results.size() - 1);
			DynamoTable.mapper.delete(a);
			a = load(a.getWord(), a.getId());
			System.out.println(a);
		}
	}

}
