package wikiParser.util;


import wmr.util.LzmaCompressor;
import wmr.util.LzmaDecompresser;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.OutputStream;

import org.junit.Test;

import SevenZip.LzmaAlone;

public class TestLzmaEncoder {
	String CONTENTS = "foo bar";
	@Test public void testPipe() throws Exception {
		File tmpIn = File.createTempFile("lzmaIn", null);
		File tmpOut = File.createTempFile("lzmaOut", null);
		
		tmpIn.deleteOnExit();
		tmpOut.deleteOnExit();
		
		FileWriter writer = new FileWriter(tmpIn);
		writer.write(CONTENTS);
		writer.flush();
		writer.close();
		
		LzmaCompressor compressor = new LzmaCompressor(CONTENTS.length());
		OutputStream compressorStream = compressor.compress();
		compressorStream.write(CONTENTS.getBytes());
		compressorStream.close();
		compressor.cleanup();
		byte[] compressed = compressor.getCompressed();
		
		LzmaDecompresser decompressor = new LzmaDecompresser(compressed, compressed.length);
		InputStream stream = decompressor.decompress();
		for (int i = 0; i < CONTENTS.length(); i++) {
			assertEquals(stream.read(), CONTENTS.charAt(i));
		}
		assertEquals(stream.read(), -1);
	}
}
