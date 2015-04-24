package Utils;

import java.math.BigInteger;

public class BinaryUtils {

	public static BigInteger fromHexToBigInteger(String hex) {
		return new BigInteger(hex, 16);
	}
	
	public static byte[] fromHex(String hex) {
		return fromHexToBigInteger(hex).toByteArray();
	}
}
