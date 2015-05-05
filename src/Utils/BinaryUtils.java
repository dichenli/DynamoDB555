package Utils;

import java.math.BigInteger;

// TODO: Auto-generated Javadoc
/**
 * The Class BinaryUtils.
 */
public class BinaryUtils {
	
	/**
	 * From decimal to big integer.
	 *
	 * @param hex the hex
	 * @return the big integer
	 */
	public static BigInteger fromDecimalToBigInteger(String hex) {
		return new BigInteger(hex);
	}
	
	/**
	 * convert from string representation of decimal bytes to byte[].
	 *
	 * @param decimalString the decimal string
	 * @return the byte[]
	 */
	public static byte[] fromDecimal(String decimalString) {
		return fromDecimalToBigInteger(decimalString).toByteArray();
	}

	/**
	 * From hex to big integer.
	 *
	 * @param hex the hex
	 * @return the big integer
	 */
	public static BigInteger fromHexToBigInteger(String hex) {
		return new BigInteger(hex, 16);
	}
	
	/**
	 * convert from string representation of hex bytes to byte[].
	 *
	 * @param hex the hex
	 * @return the byte[]
	 */
	public static byte[] fromHex(String hex) {
		return fromHexToBigInteger(hex).toByteArray();
	}
	
	/**
	 * From base64.
	 *
	 * @param base64 the base64
	 * @return the byte[]
	 */
	public static byte[] fromBase64(String base64) {
		return fromBase64ToBigInteger(base64).toByteArray();
	}
	
	/**
	 * From base64 to big integer.
	 *
	 * @param base64 the base64
	 * @return the big integer
	 */
	public static BigInteger fromBase64ToBigInteger(String base64) {
		return new BigInteger(base64, 64);
	}
	
	//failed, can't go as high as base 64
//	public static String toBase64String(byte[] bytes) {
//		return new BigInteger(bytes).toString(64);
//	}
	
//	public static String byteArrayToString(byte[] array) {
//		StringBuilder sb = new StringBuilder();
//		for(byte b : array) {
//    		sb.append(b);
//    	}
//       return sb.toString();
//	}
	
	/**
	 * Byte array to decimal string.
	 *
	 * @param array the array
	 * @return the string
	 */
	public static String byteArrayToDecimalString(byte[] array) {
		BigInteger i = new BigInteger(array);
		return i.toString();
	}
	
	/**
	 * Byte array to hex string.
	 *
	 * @param array the array
	 * @return the string
	 */
	public static String byteArrayToHexString(byte[] array) {
		BigInteger i = new BigInteger(array);
		return i.toString(16);
	}
	
	/**
	 * Array equals.
	 *
	 * @param a the a
	 * @param b the b
	 * @return true, if successful
	 */
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
	
	/**
	 * To object.
	 *
	 * @param array the array
	 * @return the byte[]
	 */
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
	 * convert Bye[] to byte[], takes O(n) time complexity.
	 *
	 * @param array the array
	 * @return the byte[]
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
	 * get a bit from the int "b" with given position "pos".
	 *
	 * @param b the b
	 * @param pos the pos
	 * @return 0 or 1
	 */
	public static int getBit(int b, int pos) {
		if(pos <= 0 || pos > 32) {
			throw new IllegalArgumentException();
		}
		
		return (b >>> (pos - 1)) & 1;
	}
	
	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {
		String decimal = "1000016645993763646612612913273621633656175576825";
		byte[] id = fromDecimal(decimal);
		System.out.println(decimal);
		System.out.println(byteArrayToDecimalString(id));
	}
}
