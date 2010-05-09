

package wikiParser.runners;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import javax.xml.stream.XMLStreamException;
import wikiParser.PageParser;
import wikiParser.Revision;
import wikiParser.mapReduce.util.MapReduceUtils;
import wikiParser.util.LzmaPipe;

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
//              System.out.println("contents is " + SevenUnzip.unzip(line.substring(i+1).getBytes()));
                processLine(value);
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


    public void processLine(byte[] escaped) {
        LzmaPipe pipe = null;
        try {
            byte[] unescaped = MapReduceUtils.unescape(escaped, escaped.length);
            pipe = new LzmaPipe(unescaped);
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
        System.out.println("number of revisions is " + i);
    }
    
    public void cleanup() throws IOException {
    	
    }

	public static void run() throws IOException {
		WikiLineReader wlr = new WikiLineReader(new File("runners_data/big.txt"));
        wlr.readLines();
	}

    public static void main(String args[]) throws IOException {
        run();
    }
}
