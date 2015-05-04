/**
 * 
 */
package Utils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author dichenli
 *
 */
public class ArrayUtils {

	public static String[] toArray(List<String> list) {
		return list.toArray(new String[list.size()]);
	}
}
