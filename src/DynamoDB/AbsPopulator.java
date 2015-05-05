package DynamoDB;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Scanner;

import Utils.IOUtils;

import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;

// TODO: Auto-generated Javadoc
/**
 * The Class AbsPopulator.
 */
public abstract class AbsPopulator {
	
	/** The read capacity. */
	static long readCapacity = 1L;
	
	/** The write capacity. */
	static long writeCapacity = 1000L;
	
	/** The input. */
	File input;

	
	/**
	 * Gets the table name.
	 *
	 * @return the table name
	 */
	public abstract String getTableName();
	
	/**
	 * Creates the table request.
	 *
	 * @return the creates the table request
	 */
	public abstract CreateTableRequest createTableRequest();
	
	/**
	 * Creates the table.
	 *
	 * @throws Exception the exception
	 */
	public abstract void createTable() throws Exception;
	
	/**
	 * Parses the input.
	 *
	 * @param line the line
	 * @return the object
	 */
	public abstract Object parseInput(String line);

	
	/**
	 * Populate.
	 */
	public void populate() {
		long total = IOUtils.countLines(input); 
		Scanner sc = IOUtils.getScanner(input);
		long count = 0;
		long current = 0;
		long begin = new Date().getTime();
		long last = begin;
		long failed = 0;
		
		ArrayList items = new ArrayList();
		if(sc == null) {
			throw new NullPointerException();
		}
		while(sc.hasNextLine()) {
			Object item = parseInput(sc.nextLine());
			if(item != null) {
				items.add(item);
				if(items.size() >= 25) {
					failed += DynamoTable.batchInsert(items);
					count += items.size();
					current += items.size();
					items = new ArrayList();
				}
			}
			
			if(current >= 500) {
				long now = new Date().getTime();
				float time1 = (float)(now - begin) ;
				float time = (float)(now - last) ;
				if(time < 1) {
					time = 1;
				}
				if(time1 < 1) {
					time1 = 1;
				}
				System.out.println("======" + count + ", total speed:" 
				+ (((float)count / time1) *1000) + "item/sec, current speed: " 
				+ (((float)current / time) *1000) + "item/sec, " 
				+ ((float)count /(float) total) + "%, failed: "
				+ failed +"======");
				current = 0;
				last = now;
			}
		}
		sc.close();
	}

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws Exception the exception
	 */
	public static void main(String[] args) throws Exception {
		if(args.length != 5 || args[0].equals("")) {
			System.out.println("Usage: <DocURL> <Anchor> <InvertedIndex> <PageRank> <IDF>, use \"no\" to mean you don't need to populate this one");
		}
		
		if(!args[0].equals("no")) DocURLPopulator.main(Arrays.copyOfRange(args, 0, 1));
		if(!args[1].equals("no")) AnchorPopulator.main(Arrays.copyOfRange(args, 1, 2));
		if(!args[2].equals("no")) InvertedIndexPopulator.main(Arrays.copyOfRange(args, 2, 3));
		if(!args[3].equals("no")) PageRankPopulator.main(Arrays.copyOfRange(args, 3, 4));
		if(!args[4].equals("no")) IDFPopulator.main(Arrays.copyOfRange(args, 4, 5));
		/*
		 * java -jar populator.jar /home/bitnami/jars/DocURL.txt /home/bitnami/jars/inverted_index /home/bitnami/jars/Anchor.txt /home/bitnami/jars/PageRank.txt /home/bitnami/jars/idf
		 * java -jar populator.jar no no no no /home/bitnami/jars/idf
		 */
	}
}
