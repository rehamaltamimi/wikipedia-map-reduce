package wikiParser.util;


import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;

import org.junit.Test;

import SevenZip.LzmaAlone;

public class TestLzmaPipe {
	@Test public void testPipe() throws Exception {
		File tmpIn = File.createTempFile("lzmaIn", null);
		File tmpOut = File.createTempFile("lzmaOut", null);
		File tmpTest = File.createTempFile("lzmaTest", null);
		
		tmpIn.deleteOnExit();
		tmpOut.deleteOnExit();
		tmpTest.deleteOnExit();
		
		FileWriter writer = new FileWriter(tmpIn);
		writer.write("foo bar");
		writer.flush();
		writer.close();
		
		LzmaAlone.main(new String[] {"e", tmpIn.getAbsolutePath(), tmpOut.getAbsolutePath()});
		
		byte [] contents = new byte[(int) tmpOut.length()];
		FileInputStream stream = new FileInputStream(tmpOut);
		int r = stream.read(contents);
		assertEquals(r, contents.length);
		
		LzmaPipe pipe = new LzmaPipe(contents);
		InputStream input = pipe.decompress();
		assertEquals(input.read(), 'f');
		assertEquals(input.read(), 'o');
		assertEquals(input.read(), 'o');
		assertEquals(input.read(), ' ');
		assertEquals(input.read(), 'b');
		assertEquals(input.read(), 'a');
		assertEquals(input.read(), 'r');
		assertEquals(input.read(), -1);
	}

}
