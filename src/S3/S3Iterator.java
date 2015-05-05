package S3;
import java.io.IOException;
import java.util.Iterator;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

// TODO: Auto-generated Javadoc
/**
 * mimic the behavior of an iterator to iterate an bucket of S3.
 *
 * @author dichenli
 */
public class S3Iterator {

	/** The bucket name. */
	private String bucketName;
	
	/** The prefix. */
	private String prefix;
	
	/** The delimiter. */
	private String delimiter;
	
	/** The list objects request. */
	private ListObjectsRequest listObjectsRequest;
	
	/** The object listing. */
	private ObjectListing objectListing;
	
	/** The iter. */
	private Iterator<S3ObjectSummary> iter;

	/**
	 * Gets the next list.
	 *
	 * @return the next list
	 * @throws Exception the exception
	 */
	private void getNextList() throws Exception {
		try {
			objectListing = S3Account.s3.listObjects(listObjectsRequest);
			iter = objectListing.getObjectSummaries().iterator();
		} catch (AmazonServiceException ase) {
			S3Account.printException(ase);
			throw ase;
		} catch (AmazonClientException ace) {
			S3Account.printException(ace);
			throw ace;
		} catch (Exception e) {
			throw e;
		}
 	}

	/**
	 * Instantiates a new s3 iterator.
	 *
	 * @param bucketName the bucket name
	 * @param prefix the prefix
	 */
	public S3Iterator(String bucketName, String prefix) {
		if(!S3Account.initialized()) {
			S3Account.init();
		}

		this.bucketName = bucketName;
		this.prefix = prefix;
		this.delimiter = "/";

		listObjectsRequest = new ListObjectsRequest()
		.withBucketName(bucketName)
		.withPrefix(prefix)
		.withDelimiter(delimiter);
		try {
			getNextList();
		} catch (Exception e) {
			e.printStackTrace();
			objectListing = null;
			iter = null;
		}
	}

	/**
	 * get objects in a given bucket and prefix. This function behave like a "iterator" function
	 * which has "nextList" and "hasNext" methods, once the nextList() is called, continue 
	 * calling the method won't retrieve the previous information again
	 * example: bucketName = "mapreduce-result", prefix = "1000files/part", 
	 */
	private void nextList() {
		if(objectListing == null) {
			//			return null;
			return;
		}
		//			ObjectListing objList = this.objectListing;
		if(objectListing.isTruncated()) {
			listObjectsRequest.setMarker(objectListing.getNextMarker());
			try {
				getNextList();
			} catch (Exception e) {
				e.printStackTrace();
				objectListing = null;
				iter = null;
			}
		} else {
			objectListing = null;
			iter = null;
		}
		//			return objList;
	}

	/**
	 * get the next file summary from s3.
	 *
	 * @return next file summary, or null if no next file exists
	 */
	public S3ObjectSummary next() {
		if(!hasNext()) {
			return null;
		}
		if(!iter.hasNext()) {
			nextList();
		}
		return iter.next();
	}

	/**
	 * next file (object) exists for the given bucket name and prefix.
	 *
	 * @return true, if successful
	 */
	public boolean hasNext() {
		if (objectListing == null || iter == null) {//first request
			return false;
		}
		if(!iter.hasNext() && !objectListing.isTruncated()) {
			return false;
		}
		return true;
	}

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static void main(String[] args) throws IOException {
		S3Iterator s3Iterator = new S3Iterator("crawler-content", "content/");
		while(s3Iterator.hasNext()) {
			//			System.out.println("next");
			S3ObjectSummary objectSummary = s3Iterator.next();
			System.out.println(objectSummary.getKey());
		}
	}
}