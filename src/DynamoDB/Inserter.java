/**
 * 
 */
package DynamoDB;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import S3.S3FileReader;
import S3.S3Iterator;
import Utils.TimeUtils;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper.FailedBatch;
import com.amazonaws.services.s3.model.S3ObjectSummary;

// TODO: Auto-generated Javadoc
/**
 * The Class Inserter.
 *
 * @author dichenli
 * binded to a class T to insert items in batch manner to DynamoDB, typically a static instance
 * of the class T
 * @param <T> the generic type
 */
public class Inserter<T> {
	
	/** The items. */
	private Set<T> items = null;
	
	/**
	 * insert data to DB. It will batch the insert task, and not send 
	 * the DB request until a total of 25 items
	 * has been sent to insert queue. However, if insertNow is set to true,
	 * it will insert immediately all currently available items in the batch. 
	 * Must call init() before calling this method.
	 *
	 * @param item the item
	 * @param insertNow the insert now
	 */
	public void insert(T item, boolean insertNow) {
		if(items == null) {
			items = new HashSet<T>();
		}
//		System.out.println("insert to local buffer: " + item.toString());
		items.add(item);
	
		if(insertNow || items.size() >= 25) {
//			System.out.println("insert buffer full, flush...");
			flush();
		}
	}
	
	/**
	 * Flush.
	 */
	public void flush() {
		if (items == null) {
			return;
		}

		ArrayList<T> list = new ArrayList<T>();
		list.addAll(items);
		
		List<FailedBatch> failedBatches = batchInsert(list); //if insert failed, print error message
//		System.out.println("insert to DB # of items: " + list.size());
		if(failedBatches != null && !failedBatches.isEmpty()) {
//			System.out.println("insert error, number of failed: " + failed.size());
//			failedBatches.get(0).getException().printStackTrace();
			for(T item : list) {
				try {
					save(item);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} else {
//			System.out.println("no error on batchsave");
		}
		items = null;
	}
	
	
	/**
	 * equal to insert(item, false);
	 * must call init() before calling this method.
	 *
	 * @param item the item
	 */
	public void insert(T item) {
		insert(item, false);
	}
	
	/**
	 * must call init() before calling this method.
	 *
	 * @param items the items
	 * @return the list
	 */
	public List<FailedBatch> batchInsert(List<T> items) {
		return DynamoTable.mapper.batchSave(items);
	}
	
	/**
	 * upload a single item to DB.
	 *
	 * @param item the item
	 * @throws Exception when DB init() failed
	 */
	public void save(T item) throws Exception {
		if(DynamoTable.mapper == null) {
			DynamoTable.init();
		}
		
		DynamoTable.mapper.save(item);
	}
	
	
}
