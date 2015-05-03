/**
 * 
 */
package S3;

import java.io.BufferedReader;
import java.io.IOException;

import Utils.IOUtils;

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
	
	public static BufferedReader getReader(String path) {
		//TODO
		return null;
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
