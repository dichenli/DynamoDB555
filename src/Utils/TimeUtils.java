package Utils;

import java.util.Date;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class TimeUtils {
	
	public static Random rand;
	public static AtomicLong atomicLong;
	
	static {
		rand = new Random();
		atomicLong = new AtomicLong();
	}
	
	/**
	 * return a date with a random variation of +- millis from current time
	 * @return
	 */
	public static Date randomDate(int millis) {
		return new Date(new Date().getTime() + (long) rand.nextInt(millis * 2) - millis);
	}
	
	/**
	 * return the time difference between old date and new date in seconds. 
	 * returns negative number if old date is actually more recent
	 * @param oldDate
	 * @param newDate
	 * @return
	 */
	public static double secondsPast(Date oldDate, Date newDate) {
		return ((double)(newDate.getTime() - oldDate.getTime())) / 1000;
	}
	
	/**
	 * 
	 * @return
	 */
	public static Long timeStamp() {
		return new Date().getTime() * 10000 + (atomicLong.getAndIncrement() % 10000);
	}
}
