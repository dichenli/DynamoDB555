package Utils;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

public class URLtoDocID {
	
	public static String toBigInteger(String key) {
		try {
			MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
			messageDigest.update(key.getBytes());
			byte[] bytes = messageDigest.digest();
			Formatter formatter = new Formatter();
			for (int i = 0; i < bytes.length; i++) {
				formatter.format("%02x", bytes[i]);
			}
			String resString = formatter.toString();
			formatter.close();
			return String.valueOf(new BigInteger(resString, 16));
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return String.valueOf(new BigInteger("0", 16));
	}

}
