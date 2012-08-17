package wmr.util;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Creates a decompressed input stream from a sequence of lzma compressed bytes by
 * spawning a new thread that has it's output attached to an input stream. 
 * 
 * @author shilad
 *
 */
public class LzmaDecompresser {
    private BufferedWriter debugFile = null;

    /**
     * @uml.property  name="t"
     */
    private Thread t = null;
    private volatile boolean inCleanup = false;
    /**
     * @uml.property  name="compressed" multiplicity="(0 -1)" dimension="1"
     */
    private byte[] compressed = null;
    private int compressedLength = -1;
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
    public LzmaDecompresser(byte[] compressed) {
        this.compressed = compressed;
        compressedLength = compressed.length;
    }

    public LzmaDecompresser(byte[] compressed, int length) {
        this.compressed = compressed;
        compressedLength = length;
    }

    public LzmaDecompresser(byte[] compressed, int length, File debugPath) {
        this.compressed = compressed;
        compressedLength = length;
        try {
            debugFile = new BufferedWriter(new FileWriter(debugPath));
        } catch (IOException ex) {
            Logger.getLogger(LzmaDecompresser.class.getName()).log(Level.SEVERE, null, ex);
        }
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
                    if (!inCleanup) {
                        System.err.println("decoding lzma failed:");
                        e.printStackTrace();
                    }
                    try {
                        outputStream.close();
                    } catch (IOException e1) {
                        if (!inCleanup) {
                            e1.printStackTrace();
                        }
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
        if (inStream.read(properties, 0, propertiesSize) != propertiesSize) {
            throw new Exception("input .lzma file is too short");
        }
        SevenZip.Compression.LZMA.Decoder decoder = new SevenZip.Compression.LZMA.Decoder();
        if (!decoder.SetDecoderProperties(properties)) {
            throw new Exception("Incorrect stream properties");
        }
        long outSize = 0;
        for (int i = 0; i < 8; i++) {
            int v = inStream.read();
            if (v < 0) {
                throw new Exception("Can't read stream size");
            }
            outSize |= ((long) v) << (8 * i);
        }
        if (!decoder.Code(inStream, outputStream, outSize)) {
            throw new Exception("Error in data stream");
        }
        outputStream.flush();
        outputStream.close();
        inStream.close();
    }

    public void cleanup() {
        inCleanup = true;
        if (t != null) {
            t.stop();
            t.interrupt();
            try {
                t.join(10000);
            } catch (InterruptedException ex) {
//                            Logger.getLogger(LzmaPipe.class.getName()).log(Level.SEVERE, null, ex);
            }
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
