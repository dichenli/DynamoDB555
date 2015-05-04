/**
 * 
 */
package S3;

import indexer.WordHeat;

import java.io.BufferedReader;
import java.io.IOException;

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
	
	static String contentDir = "crawler-content/content/";
	
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
		System.out.println(path);
		
		if(!nameUtils.isLetterDigitOrHyphen(path.charAt(0))) {
			System.err.println("Ilegal first character, must be letter, number or hyphen");
			throw new IllegalArgumentException();
		}
		
		String[] splited = path.split("/", 2);
		System.out.println(splited[0]);
		System.out.println(splited[1]);
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
		System.out.println("bucketName: " + bucketName);
		System.out.println("Prefix: " + prefix);
		
		S3Iterator iter = new S3Iterator(bucketName, prefix); //find the file
		if(!iter.hasNext()) {
			System.err.println("S3FileReader.getFileReader: File not found");
			return null;
		}
		S3ObjectSummary item = iter.next();
//		if(iter.hasNext()) {
//			System.err.println("Found more than one match from the given file name!");
//			throw new IllegalArgumentException();
//		}
		return new S3FileReader(item).getStreamReader();
	}

	/**
	 * get the full content of a file from given file ID
	 * @param decimalID
	 * @return
	 */
	public static String getFileContent(String decimalID) {
		if(decimalID == null) {
			throw new NullPointerException();
		}
		String path = contentDir + decimalID;
		BufferedReader reader = S3FileReader.getFileReader(path);
		if(reader == null) {
			return null;
		}
		StringBuilder sb = new StringBuilder();
		String line = null;
		try {
			while((line = reader.readLine()) != null) {
				sb.append(line);
			}
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		return sb.toString();
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
		
		
		Thread[] t = new Thread[filename.length]; //multi-thread parallel process
		for(i = 0; i < filename.length; i++ ) {
			Runnable r = new Runnable() {
				
				@Override
				public void run() {
					String result = getFileContent(filename[i]);
					System.out.println("loaded");
					System.out.println(WordHeat.findPosition(result, query));
				}
			};
			t[i] = new Thread(r);
			t[i].start();
		}
		for(i = 0; i < filename.length; i++ ) {
			try {
				t[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	static String[] query = {"nets", "adidas"};
	static String[] filename = {"1000016645993763646612612913273621633656175576825",
			"1000033548111337632249107769061580264517257318070",
			"1000038001979123892805870222863680232910468531648",
			"1000040082942895680512083852335399031460963509561",
			"1000046092798133031507036078583383187932828878225",
			"1000063444022270000724386391770168457269810013194"};
	static int i = 0;
}
