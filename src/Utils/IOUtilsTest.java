package Utils;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.Writer;

import org.junit.Before;
import org.junit.Test;

public class IOUtilsTest {

	@Before
	public void setUp() throws Exception {
	}

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
