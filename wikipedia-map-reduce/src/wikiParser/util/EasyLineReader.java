/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package wikiParser.util;

import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.LineReader;

/**
 *
 * @author shilad
 */
public class EasyLineReader {
    public static int MAX_LINE_LENGTH = 750 * (1<<20);    // 750MB
    
    private LineReader reader;
    private FSDataInputStream fileIn;
    
    private String lastLine = null;
    private Text buffer = new Text();

    private long lineNum = 0;
    private long bytesExtracted = 0;
    private long underlyingTotalBytes = 0;
    
    private boolean eof = false;

    public EasyLineReader(Path path, Configuration job) throws IOException {
        this(path, job, -1);
    }
    
    public EasyLineReader(Path path, Configuration job, long fileLength) throws IOException {
        FileSystem fs = path.getFileSystem(job);
        if (fileLength < 0) {
            fileLength = fs.getFileStatus(path).getLen();
        }
        fileIn = fs.open(path);
        InputStream in = fileIn;


        CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(job);
        CompressionCodec codec = compressionCodecs.getCodec(path);
        underlyingTotalBytes = fileLength;
        if (codec != null) {
          in = codec.createInputStream(in);
        }
        reader = new LineReader(in, job);
    }

    public void unreadLine(String line) {
        if (this.lastLine != null) {
            throw new IllegalStateException(
                    "unreadLine called twice consecutively without call to readLine");
        }
        this.lastLine = line;
    }

    public synchronized String readLine() throws IOException {
        if (this.lastLine != null) {
            String line = this.lastLine;
            this.lastLine = null;
            return line;
        }
        if (eof) {
            return null;
        }
        lineNum += 1;


        int r = reader.readLine(buffer, MAX_LINE_LENGTH, MAX_LINE_LENGTH);
        if (r == 0) {
            eof = true;
            return null;
        }
        bytesExtracted += r;
        return buffer.toString();
    }

    public synchronized void close() throws IOException {
        if (reader != null) {
            this.reader.close();
            this.reader = null;
            this.eof = true;
        }
    }

    public long getUnderlyingTotalBytes() {
        return underlyingTotalBytes;
    }

    public long getUnderlyingBytesRead() throws IOException {
        return fileIn.getPos();
    }

    public long getBytesExtracted() {
        return bytesExtracted;
    }

    public long getLinesExtracted() {
        return lineNum;
    }
}
