

package wikiParser.runners;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.util.Arrays;
import javax.xml.stream.XMLStreamException;
import wmr.core.PageParser;
import wmr.core.Revision;
import wmr.util.LzmaDecompresser;
import wmr.util.SevenUnzip;
import wmr.util.Utils;

/**
 * Reads in a wikipedia file in the standard hadoop format.
 * One article per line with tab separated tokens.
 * First token is key, second token is escaped, lzma-encoded revision history
 * for article.
 *
 * Classes probably will extend this.
 * 
 * @author shilad
 */
public class WikiLineReader {
    /**
	 * @uml.property  name="stream"
	 */
    private final BufferedInputStream stream;

    public WikiLineReader(File path) throws IOException {
        this.stream = new BufferedInputStream(new FileInputStream(path));
    }


    public void readLines() throws IOException {
        int numLines = 0;
        while (true) {
            if (stream.available() == 0) {
                break;
            }
            try {
                String key = readKey();
                byte[] value = readValue();
                System.err.println("processing line " + numLines + ": " + key);
//              System.out.println("contents is " + SevenUnzip.unzip(value));
//                processLine(value);
                printLine(value);
            } catch (Exception e) {
                System.err.println("\nfailure:");
                e.printStackTrace();
            }
            numLines++;
        }
        cleanup();
        
    }

    public String readKey() throws IOException {
        StringBuffer key = new StringBuffer();
        while (true) {
            int b = stream.read();
            if (b < 0 || b == '\t') {
                break;
            }
            key.append((char)b);
        }
        return key.toString();
    }

    public byte[] readValue() throws IOException {
        byte[] buff = new byte[4096];
        int nUsed = 0;

        while (true) {
            int b = stream.read();
            if (b < 0 || b == '\n') {
                break;
            }
            if (nUsed == buff.length) {
                buff = Arrays.copyOfRange(buff, 0, buff.length*2);
            }
            buff[nUsed++] = (byte)b;
        }

        return Arrays.copyOfRange(buff, 0, nUsed);
    }

    public void printLine(byte[] escaped) {
        LzmaDecompresser pipe = null;
        try {
            byte[] unescaped = Utils.unescape(escaped, escaped.length);
            System.err.println("unescaped length is " + unescaped.length);
            pipe = new LzmaDecompresser(unescaped);
            PipedInputStream in = pipe.decompress();
            byte[] buff = new byte[80];
            while (true) {
                int r = in.read(buff);
                if (r <= 0) {
                    break;
                }
                System.out.println("read " + new String(buff));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (pipe != null) {
                pipe.cleanup();
            }
        }
        
    }

    public void processLine(byte[] escaped) {
        LzmaDecompresser pipe = null;
        try {
            byte[] unescaped = Utils.unescape(escaped, escaped.length);
            System.err.println("unescaped length is " + unescaped.length);
            pipe = new LzmaDecompresser(unescaped);
            PageParser parser = new PageParser(pipe.decompress());
            processArticle(parser);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (pipe != null) {
                pipe.cleanup();
            }
        }
    }

    public void processArticle(PageParser parser) throws XMLStreamException, IOException {
        int i = 0;
        while (true) {
            Revision r = parser.getNextRevision();
            if (r == null)
                break;
            i++;
        }
        System.err.println("number of revisions is " + i);
    }
    
    public void cleanup() throws IOException {
    	
    }

	public static void run() throws IOException {
		WikiLineReader wlr = new WikiLineReader(new File("/Users/research/wikipedia.txt.tiny"));
        wlr.readLines();
	}

    public static void main(String args[]) throws IOException {
        run();
    }
}
