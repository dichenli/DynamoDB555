/**
 * 
 */
package Utils;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @author dichenli
 * utility to calculate SHA-1 from input string
 */
public class SHAUtils {
	MessageDigest md;


	public SHAUtils() {
		try {
			md = MessageDigest.getInstance("SHA-1");
		} catch (NoSuchAlgorithmException e) {
			//Shouldn't let it happen. If happens, debug it before running again
			e.printStackTrace();
		}
	}

	/**
	 * calculate SHA-1 value from input key as string 
	 * @param key
	 * @return
	 */
	public byte[] hash(String key) {
		md.reset();
		md.update(key.getBytes());
		return md.digest();
	}
	
	/**
	 * hash and then get the string representation of the hashed value
	 * @param key
	 * @return
	 */
	public String hashToString(String key) {
		byte[] value = hash(key);
		return bytesToString(value);
	}
	
	/**
	 * from bytes to its Hex representation
	 * @param bytes
	 * @return
	 */
	public static String bytesToString(byte[] bytes) {
	    StringBuilder sb = new StringBuilder();
	    for (byte b : bytes) {
	        sb.append(String.format("%02X", b));
	    }
	    return sb.toString();
	}
	
	/**
	 * call hash(key), then mod the value to give a index between 0 to buckets - 1
	 * @param key
	 * @param buckets
	 * @return
	 */
	public int hashMod(String key, int buckets) {
		byte[] bytes = hash(key);
		BigInteger value = new BigInteger(bytes);
		BigInteger base = BigInteger.valueOf(buckets);
		BigInteger result = value.mod(base);
		return result.intValue();
	}
	
	/**
	 * if you have four workers, the first worker should handle all the keys
	 * that hash to the range 0x0000...0x3FFF, the second worker should handle 
	 * the keys that hash to the range 0x4000...0x7FFF, the third should 
	 * handle 0x8000...0xBFFF, and the fourth should handle 0xC000...0xFFFF.
	 * @param key
	 * @param buckets
	 * @return splited number, from 1 to buckets. 1 corresponds to lowest range
	 * of values
	 */
	public int hashSplit(String key, int buckets) {
		byte[] bytes = hash(key);
		return split(bytes, buckets);
	}
	
	public static int split(byte[] bytes, int buckets) {
		byte[] max = new byte[bytes.length + 1];
		max[0] = 1; 
		for(int i = 1; i < bytes.length; i++) {
			max[i] = 0;
		}
		BigInteger value = new BigInteger(unsigned(bytes));
		BigInteger maxInt = new BigInteger(max);
		BigInteger base = BigInteger.valueOf(buckets);
		BigInteger range = maxInt.divide(base);
		BigInteger result = value.divide(range);
		System.out.println("\nvalue: " + value.toString()
				+ "\nmaxInt: " + maxInt.toString()
				+ "\nbase: " + base.toString()
				+ "\nrange: " + range.toString()
				+ "\nresult: " + result.toString() + " + 1");
		return result.intValue();
	}
	
	/**
	 * convert a negative number represented by bytes to an unsigned number
	 * example: 0xFFFF will be converted to 0x00FFFF, so that it's unsigned
	 * @param bytes
	 * @return
	 */
	public static byte[] unsigned(byte[] bytes) {
		byte[] newNum = new byte[bytes.length + 1];
		newNum[0] = 0;
		for(int i = 0; i < bytes.length; i++) {
			newNum[i + 1] = bytes[i];
		}
		return newNum;
	}
}
