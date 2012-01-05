/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package wmr.core;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import wikiParser.util.EasyLineReader;

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

        private EasyLineReader reader = null;

        private static String HEADER = null;
        private static String FOOTER = "</mediawiki>";
        private static String START_PAGE = "<page>";
        private static String END_PAGE = "</page>";


        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            this.split = (FileSplit) split;
            assert(this.split.getStart() == 0);
            reader = new EasyLineReader(this.split.getPath(), context.getConfiguration(), this.split.getLength());

            this.context = context;
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
//            System.err.println("HERE 3");
            // find the start page marker
            while (true) {
                String line = reader.readLine();
                if (line == null) {
                    return false;
                }
                if (HEADER == null) {
                    HEADER = line;
                }
//                System.err.println("read ========" + line + "=======");
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
                String line = reader.readLine();
                if (line == null) {
                    return false;
                }
//                System.err.println("line is ====" + buff + "=====");
                buff.append(line);
                buff.append("\n");
                if (line.trim().equals(END_PAGE)) {
                    break;
                }
            }

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
            return true;
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
            long l = reader.getUnderlyingTotalBytes();
            long n = reader.getUnderlyingBytesRead();
            return (l == 0) ? 0.0f : (1.0f * n / l);
        }

        @Override
        public synchronized void close() throws IOException {
            this.reader.close();
        }
    }


}
