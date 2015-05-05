package Utils;

import java.util.Date;
import java.util.Random;

// TODO: Auto-generated Javadoc
/**
 * The Class TimeUtils.
 */
public class TimeUtils {
	
	/** The rand. */
	public static Random rand;
	
	/**
	 * return a date with a random variation of +- millis from current time.
	 *
	 * @param millis the millis
	 * @return the date
	 */
	public static Date randomDate(int millis) {
		return new Date(new Date().getTime() + (long) rand.nextInt(millis * 2) - millis);
	}
	
	/**
	 * return the time difference between old date and new date in seconds. 
	 * returns negative number if old date is actually more recent
	 *
	 * @param oldDate the old date
	 * @param newDate the new date
	 * @return the double
	 */
	public static double secondsPast(Date oldDate, Date newDate) {
		return ((double)(newDate.getTime() - oldDate.getTime())) / 1000;
	}
}
