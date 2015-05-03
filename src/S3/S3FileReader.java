/**
 * 
 */
package S3;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Scanner;

import javafx.scene.shape.Line;
import Utils.IOUtils;
import Utils.nameUtils;

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * @author dichenli
 * read file lines from S3 bucket
 */
public class S3FileReader {
	
	S3ObjectSummary s3Summary;
	
	public S3FileReader(S3ObjectSummary obj) {
		this.s3Summary = obj;
	}

	/**
	 * get the bufferedReader of a file (i.e. S3 object) from the given S3ObjectSummary object
	 * by making remote function call to the S3 server. 
	 * @return the buffered reader, or null if exception thrown
	 */
	public BufferedReader getStreamReader() {
		try {
			return IOUtils.getReader(getObjectStream());
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	S3ObjectInputStream getObjectStream() {
		return getObject().getObjectContent();
	}
	
	S3Object getObject() {
		String bucketName = s3Summary.getBucketName();
		String key = s3Summary.getKey();
		return S3Account.s3.getObject(new GetObjectRequest(bucketName, key));
	}
	
	/**
	 * split a S3 path to a bucket name and a prefix, the path must be a valid
	 * S3 path with "bucketname/following-paths" format
	 * @param path
	 * @return
	 */
	public static String[] splitPath(String path) {
		if(path == null) {
			throw new IllegalArgumentException();
		}
		
		if(!nameUtils.isLetterDigitOrHyphen(path.charAt(0))) {
			System.err.println("Ilegal first character, must be letter, number or hyphen");
			throw new IllegalArgumentException();
		}
		
		String[] splited = path.split("/", 2);
		if(splited.length != 2) {
			System.err.println("Path must contain / to separate bucket");
			throw new IllegalArgumentException();
		}
		
		return splited;
	}
	
	/**
	 * get a buffered reader of a file on S3 from given path.
	 * returns null if no results were found
	 * @throws IllegalArgumentException if more than one file found which
	 * match result
	 * @param path
	 * @return
	 */
	public static BufferedReader getFileReader(String path) {
		String[] splited = splitPath(path);
		String bucketName = splited[0];
		String prefix = splited[1];
		
		S3Iterator iter = new S3Iterator(bucketName, prefix); //find the file
		if(!iter.hasNext()) {
			System.err.println("S3FileReader.getFileReader: File not found");
			return null;
		}
		S3ObjectSummary item = iter.next();
		if(iter.hasNext()) {
			System.err.println("Found more than one match from the given file name!");
			throw new IllegalArgumentException();
		}
		return new S3FileReader(item).getStreamReader();
	}

	public static void main(String[] args) throws IOException {
//		S3Iterator s3Iterator = new S3Iterator("crawler-content", "content/11336");
//		while(s3Iterator.hasNext()) {
//			//			System.out.println("next");
//			S3ObjectSummary objectSummary = s3Iterator.next();
//			BufferedReader reader = IOUtils.getReader(getObjectStream(objectSummary));
//			String line = null;
//			while((line = reader.readLine()) != null) {				
//				System.out.println(line);
//			}
//			reader.close();
//		}
	}
}
