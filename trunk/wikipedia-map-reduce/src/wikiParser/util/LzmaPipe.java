package wikiParser.util;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

/**
 * Creates a decompressed input stream from a sequence of lzma compressed bytes by
 * spawning a new thread that has it's output attached to an input stream. 
 * 
 * @author shilad
 *
 */
public class LzmaPipe {
	/**
	 * @uml.property  name="t"
	 */
	private Thread t = null;
	/**
	 * @uml.property  name="compressed" multiplicity="(0 -1)" dimension="1"
	 */
	private byte [] compressed = null;
        private int compressedLength = -1;
        
	/**
	 * @uml.property  name="inputStream"
	 */
	private PipedInputStream inputStream;
	/**
	 * @uml.property  name="outputStream"
	 */
	private PipedOutputStream outputStream;
	
	public LzmaPipe(byte [] compressed) {
		this.compressed = compressed;
                compressedLength = compressed.length;
	}
	
	public LzmaPipe(byte [] compressed, int length) {
		this.compressed = compressed;
                compressedLength = length;
	}
	
	/**
	 * Creates a new decompressed input stream for the sequence of bytes.
	 * @return
	 * @throws IOException
	 */
	public PipedInputStream decompress() throws IOException {
		if (t != null) {
			throw new IllegalStateException();
		}
		final PipedInputStream inputStream = new PipedInputStream();
		final PipedOutputStream outputStream = new PipedOutputStream(inputStream);
		this.inputStream = inputStream;
		this.outputStream = outputStream;
		t = new Thread() {
			public void run() {
				try {
					launchDecompressor(outputStream);
				} catch (Exception e) {
					System.err.println("decoding lzma failed:");
					e.printStackTrace();
					try {
						outputStream.close();
					} catch (IOException e1) {
						e1.printStackTrace();
					}
				}
			}
		};
		t.start();
		return inputStream;
	}
	
	private void launchDecompressor(PipedOutputStream outputStream) throws Exception {
		ByteArrayInputStream byteStream = new ByteArrayInputStream(compressed, 0, compressedLength);
		BufferedInputStream inStream = new BufferedInputStream(byteStream);
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
		if (!decoder.Code(inStream, outputStream, outSize))
			throw new Exception("Error in data stream");
		outputStream.flush();
		outputStream.close();
		inStream.close();
	}
	
	public void cleanup() {
		if (t != null) {
			t.stop();
			t = null;
			try {
				inputStream.close();
				outputStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
