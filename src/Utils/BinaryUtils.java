package Utils;

import java.math.BigInteger;

public class BinaryUtils {
	
	public static BigInteger fromDecimalToBigInteger(String hex) {
		return new BigInteger(hex);
	}
	
	/**
	 * convert from string representation of decimal bytes to byte[]
	 * @throws NumberFormatException
	 * @param hex
	 * @return
	 */
	public static byte[] fromDecimal(String decimalString) {
		return fromDecimalToBigInteger(decimalString).toByteArray();
	}

	public static BigInteger fromHexToBigInteger(String hex) {
		return new BigInteger(hex, 16);
	}
	
	/**
	 * convert from string representation of hex bytes to byte[]
	 * @throws NumberFormatException
	 * @param hex
	 * @return
	 */
	public static byte[] fromHex(String hex) {
		return fromHexToBigInteger(hex).toByteArray();
	}
	
	public static byte[] fromBase64(String base64) {
		return fromBase64ToBigInteger(base64).toByteArray();
	}
	
	public static BigInteger fromBase64ToBigInteger(String base64) {
		return new BigInteger(base64, 64);
	}
	
	//failed, can't go as high as base 64
//	public static String toBase64String(byte[] bytes) {
//		return new BigInteger(bytes).toString(64);
//	}
	
	public static String byteArrayToString(byte[] array) {
		StringBuilder sb = new StringBuilder();
		for(byte b : array) {
    		sb.append(b);
    	}
       return sb.toString();
	}
	
	public static boolean arrayEquals(byte[] a, byte[] b) {
		if(a == b) {
			return true;
		} else if (a == null || b == null) {
			return false;
		} else if (a.length != b.length) {
			return false;
		} else {
			for(int i = 0; i < a.length; i++) {
				if(a[i] != b[i]) {
					return false;
				}
			}
			return true;
		}
	}
	
	public static Byte[] toObject(byte[] array) {
		if(array == null) {
			return null;
		}
		Byte[] arr2 = new Byte[array.length];
		for(int i = 0; i < array.length; i++) {
			arr2[i] = array[i];
		}
		return arr2;
	}
	
	/**
	 * convert Bye[] to byte[], takes O(n) time complexity
	 * @param array
	 * @throws NullPointerException if any element of array is null, but returns null
	 * if array is null
	 * @return
	 */
	public static byte[] toPrimary(Byte[] array) {
		if(array == null) {
			return null;
		}
		byte[] arr2 = new byte[array.length];
		for(int i = 0; i < array.length; i++) {
			arr2[i] = array[i]; //NullPointerException may be thrown
		}
		return arr2;
	}
	
	/**
	 * get a bit from the int "b" with given position "pos"
	 * @param b: a byte number
	 * @param pos: a position, 1 ... 32, (1 means the least significant bit)
	 * @return 0 or 1
	 */
	public static int getBit(int b, int pos) {
		if(pos <= 0 || pos > 32) {
			throw new IllegalArgumentException();
		}
		
		return (b >>> (pos - 1)) & 1;
	}
}
