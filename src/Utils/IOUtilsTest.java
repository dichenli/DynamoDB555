package Utils;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.Writer;

import org.junit.Before;
import org.junit.Test;

// TODO: Auto-generated Javadoc
/**
 * The Class IOUtilsTest.
 */
public class IOUtilsTest {

	/**
	 * Sets the up.
	 *
	 * @throws Exception the exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * Test rename.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test
	public void testRename() throws IOException {
		File file = new File("/Users/dichenli/test.abc");
		if(!IOUtils.fileExists(file)) {
			IOUtils.createFile(file);
		}
		Writer w = IOUtils.getWriter(file);
		w.write("abc");
		w.close();
		file = IOUtils.rename(file, "/Users/dichenli/test.abc.txt");
		assertEquals("txt", IOUtils.getExtension(file));
		file = IOUtils.appendExtension(file, ".done");
		assertEquals("done", IOUtils.getExtension(file));
	}

}
