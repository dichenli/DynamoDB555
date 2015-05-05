/**
 * 
 */
package Utils;

import java.util.ArrayList;
import java.util.List;

// TODO: Auto-generated Javadoc
/**
 * The Class ArrayUtils.
 *
 * @author dichenli
 */
public class ArrayUtils {

	/**
	 * To array.
	 *
	 * @param list the list
	 * @return the string[]
	 */
	public static String[] toArray(List<String> list) {
		return list.toArray(new String[list.size()]);
	}
}
