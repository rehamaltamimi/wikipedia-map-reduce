package wmr.util;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Creates a compressed input stream containing a sequence of lzma compressed bytes by
 * spawning a new thread that has it's output attached to an input stream. 
 * 
 * @author shilad
 *
 */
public class LzmaCompressor {
    private BufferedWriter debugFile = null;

    /**
     * @uml.property  name="t"
     */
    private Thread t = null;
    private volatile boolean inCleanup = false;
    private long uncompressedLength = 0;
    
    /**
     * @uml.property  name="compressed" multiplicity="(0 -1)" dimension="1"
     */
    private ByteArrayOutputStream compressed = new ByteArrayOutputStream();
    /**
     * @uml.property  name="inputStream"
     */
    private PipedInputStream inputStream;
    /**
     * @uml.property  name="outputStream"
     */
    private PipedOutputStream outputStream;

    /*
     *
     */
    public LzmaCompressor(int uncompressedLength) {
    	this.uncompressedLength = uncompressedLength;
    }

    /**
     * Creates a new decompressed input stream for the sequence of bytes.
     * @return
     * @throws IOException
     */
    public PipedOutputStream compress() throws IOException {
        if (t != null) {
            throw new IllegalStateException();
        }
        final PipedOutputStream outputStream = new PipedOutputStream();
        final PipedInputStream inputStream = new PipedInputStream(outputStream);
        this.inputStream = inputStream;
        this.outputStream = outputStream;
        t = new Thread() {

            public void run() {
                try {
                    launchCompressor(inputStream);
                } catch (Exception e) {
                    if (!inCleanup) {
                        System.err.println("decoding lzma failed:");
                        e.printStackTrace();
                    }
                    try {
                        inputStream.close();
                    } catch (IOException e1) {
                        if (!inCleanup) {
                            e1.printStackTrace();
                        }
                    }
                }
            }
        };
        t.start();
        return outputStream;
    }

    private void launchCompressor(PipedInputStream inputStream) throws Exception {
        SevenZip.Compression.LZMA.Encoder encoder = new SevenZip.Compression.LZMA.Encoder();
		encoder.SetEndMarkerMode(false);
		encoder.WriteCoderProperties(compressed);
		for (int i = 0; i < 8; i++) {
			compressed.write((int)(uncompressedLength >>> (8 * i)) & 0xFF);
		}
		encoder.Code(inputStream, compressed, -1, -1, null);
		compressed.flush();
    }

    public synchronized void cleanup() {
        inCleanup = true;
        if (t != null) {
            try {
            	if (t.isAlive()) {
	                t.join(2000);	// first wait for 2 seconds
            	}
            	if (t.isAlive()) {
                    t.stop();
                    t.interrupt();
                    try {
                        t.join(10000);
                    } catch (InterruptedException ex2) {
                    }            		
            	}
            } catch (InterruptedException ex) {
            }
            t = null;
            try {
                outputStream.close();
                inputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    
    public byte[] getCompressed() {
    	cleanup();
    	return compressed.toByteArray();
    }
}
