package wmr.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class SevenUnzip {
	
	/*
	 * This class takes a .7zip file, "7za e"s it and runs through the
	 * file removing tabs and newlines, and replacing each newline/tab
	 * sequence with a "\s".  This creates a whole bunch of single line
	 * XML files.
	 * 
	 * These are used as input to a collating map/reduce job.
	 * 
	 */
	
	public static String unzip(byte [] input) throws IOException, Exception {
		ByteArrayInputStream byteStream = new ByteArrayInputStream(input);
		BufferedInputStream inStream = new BufferedInputStream(byteStream);
		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		BufferedOutputStream outStream = new BufferedOutputStream(bytesOut);
		int propertiesSize = 5;
		byte[] properties = new byte[propertiesSize];
		if (inStream.read(properties, 0, propertiesSize) != propertiesSize)
			throw new Exception("input .lzma file is too short");
		SevenZip.Compression.LZMA.Decoder decoder = new SevenZip.Compression.LZMA.Decoder();
		if (!decoder.SetDecoderProperties(properties))
			throw new Exception("Incorrect stream properties");
		long outSize = 0;
		for (int i = 0; i < 8; i++)
		{
			int v = inStream.read();
			if (v < 0)
				throw new Exception("Can't read stream size");
			outSize |= ((long)v) << (8 * i);
		}
		if (!decoder.Code(inStream, outStream, outSize))
			throw new Exception("Error in data stream");
		outStream.flush();
		outStream.close();
		inStream.close();
		return bytesOut.toString("UTF-8");
	}

}
