/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package wikiParser;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.xml.stream.XMLStreamException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

/**
 *
 * @author shilad
 */
public class CurrentRevisionInputFormat extends FileInputFormat<Long, CurrentRevision> {
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public RecordReader<Long, CurrentRevision> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        WPRecordReader reader = new WPRecordReader();
        reader.initialize(split, context);
        return reader;
    }

    /**
     * Most of this code borrowed from LineRecordReader
     */
    public static class WPRecordReader extends RecordReader<Long, CurrentRevision> {

        private FileSplit split;
        private TaskAttemptContext context;
        private Long key;
        private CurrentRevision value;
        private long start;
        private long end;
        private long pos;
        private LineReader reader;
        private int maxLineLength = Integer.MAX_VALUE;
        private boolean atEof = false;
        private String lineBuffer = null;

        private static String HEADER = null;
        private static String FOOTER = "</mediawiki>";
        private static String START_PAGE = "<page>";
        private static String END_PAGE = "</page>";


        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            this.split = (FileSplit) split;
            Configuration job = context.getConfiguration();
            final Path file = this.split.getPath();

            assert(this.split.getStart() == 0);
            this.context = context;
            start = 0;
            end = this.split.getLength();
            pos = 0;

            assert(start == 0);

            FileSystem fs = file.getFileSystem(job);
            FSDataInputStream fileIn = fs.open(this.split.getPath());
            InputStream in = fileIn;


            CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(job);
            CompressionCodec codec = compressionCodecs.getCodec(file);
            if (codec != null) {
              in = codec.createInputStream(in);
              end = Integer.MAX_VALUE;
            }

            reader = new LineReader(in, job);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (atEof) {
                return false;
            }

//            System.err.println("HERE 3");
            // find the start page marker
            Text text = new Text();
            while (true) {
                if (!readLine(text)) {
                    return false;
                }
                String line = text.toString();
                if (HEADER == null) {
                    HEADER = line;
                }
                if (line.trim().equals(START_PAGE)) {
                    break;
                }
            }
//            System.err.println("HERE 4");

            // Intialize buffer
            StringBuilder buff = new StringBuilder();
            buff.append(HEADER + "\n");
            buff.append(START_PAGE + "\n");

            // Append lines until end of page marker
            while (true) {
                if (!readLine(text)) {
                    return false;
                }
                String line = text.toString();
//                System.err.println("line is ====" + buff + "=====");
                buff.append(line);
                buff.append("\n");
                if (line.trim().equals(END_PAGE)) {
                    break;
                }
            }
//            System.err.println("HERE 5");

//            System.err.println("header is " + HEADER + "\n=================");
//            System.err.println("buff is " + buff);
            byte bytes[] = buff.toString().getBytes("UTF-8");
            InputStream stream = new ByteArrayInputStream(bytes);
            try {
                PageParser parser = new PageParser(stream);
                Page page = parser.getArticle();
                Revision rev = parser.getNextRevision();
                try {
                    key = Long.valueOf(page.getId());
                } catch(Exception e) {
                    System.err.println("invalid article id: " + page.getId());
                    key = -1L;
                }
                value = new CurrentRevision(page, rev);

            } catch (Exception ex) {
                System.err.println("error parsing record:");
                ex.printStackTrace();
                return nextKeyValue();
            }
            return !atEof;
        }

        @Override
        public Long getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public CurrentRevision getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            if (start == end) {
                return 0.0f;
            } else {
                return Math.min(1.0f, (pos - start) / (float)(end - start));
            }
        }

        @Override
        public synchronized void close() throws IOException {
            if (reader != null) {
                reader.close();
                reader = null;
            }
        }

        private void unreadLine(String line) {
            lineBuffer = line;
        }
        private boolean readLine(Text text) throws IOException {
            if (lineBuffer != null) {
                text.set(lineBuffer);
                lineBuffer = null;
//                System.err.println("read from buff ===========" + text + "=====");
                return true;
            }
            if (atEof) {
                return false;
            }
            int r = reader.readLine(
                        text, maxLineLength,
                        Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),
                        maxLineLength));
//            System.err.println("read ===========" + text + "=====");
            pos += r;
            if (r == 0) {
                atEof = true;
            }
            return !atEof;
        }
    }


}
