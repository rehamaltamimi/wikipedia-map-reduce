/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package wmr.core;

import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.stream.XMLStreamException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import wmr.util.LzmaDecompresser;
import wmr.util.Utils;

/**
 *
 * @author shilad
 */
public class AllRevisionsInputFormat extends FileInputFormat<Long, AllRevisions> {
    private static final Logger LOG = Logger.getLogger(AllRevisionsInputFormat.class.getName());
    
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return true;
    }

    @Override
    public RecordReader<Long, AllRevisions> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        WPRecordReader reader = new WPRecordReader();
        reader.initialize(split, context);
        return reader;
    }

    /**
     * Most of this code borrowed from LineRecordReader
     */
    public static class WPRecordReader extends RecordReader<Long, AllRevisions> {

        private LineRecordReader reader = null;
        private FileSplit split;
        private TaskAttemptContext context;
        private Long key;
        private AllRevisions value;
        private LzmaDecompresser pipe = null;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            reader = new LineRecordReader();
            reader.initialize(split, context);
            this.split = (FileSplit) split;
            assert(this.split.getStart() == 0);
            this.context = context;
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
//            System.err.println("HERE 3");
            // find the start page marker
            while (true) {
                if (!reader.nextKeyValue()) {
                    return false;
                }
                Text line = reader.getCurrentValue();
                if (processLine(line)) {
                    return true;
                }
            }
        }

        public boolean processLine(Text line) {
            closePipe();

            int i = line.find("\t");
            if (i < 0) {
                String beg = new String(line.getBytes(), 0, Math.min(50, line.getLength()));
                LOG.warning("Didn't find tab in line beginning with '" + beg + "'");
                return false;
            }
            Text key = new Text(new String(line.getBytes(), 0, i));

            try {
                context.progress();
                int length = Utils.unescapeInPlace(line.getBytes(), i+1, line.getLength());
                pipe = new LzmaDecompresser(line.getBytes(), length);
                PageParser parser = new PageParser(pipe.decompress());
                Page page = parser.getArticle();
                this.value = new AllRevisions(page, new RevisionIterable(context, page, parser));
                this.key = Long.valueOf(key.toString());
            } catch (Exception e) {
                context.progress();
                LOG.log(Level.SEVERE, "error when processing " + key + ":", e);
                return false;
            }
            return true;
        }


        public static class RevisionIterable implements Iterable<Revision>, Iterator<Revision> {
            private PageParser parser = null;
            private Revision revision = null;
            private Page page;
            private final TaskAttemptContext context;

            public RevisionIterable(TaskAttemptContext context, Page page, PageParser parser) {
                this.parser = parser;
                this.page = page;
                this.context = context;
                this.advance();
            }
            
            public Iterator<Revision> iterator() {
                return this;
            }
            public boolean hasNext() {
                return (revision != null);
            }
            public Revision next() {
                if (revision == null) {
                    return null;
                }
                Revision result = revision;
                advance();
                return result;
            }
            public void remove() {
                throw new UnsupportedOperationException("Not supported.");
            }
            private void advance() {
                context.progress();
                try {
                    revision = parser.getNextRevision();
                } catch (IllegalStateException ex) {
                    LOG.log(Level.SEVERE, "error in parsing revisions for " + page.getName(), ex);
                    revision = null;
                } catch (XMLStreamException ex) {
                    LOG.log(Level.SEVERE, "error in parsing revisions for " + page.getName(), ex);
                    revision = null;
                }
            }
        }

        @Override
        public Long getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public AllRevisions getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return reader.getProgress();
        }

        private void closePipe() {
            if (pipe != null) {
                try {
                    this.pipe.cleanup();
                } catch(Exception e) {

                }
                this.pipe = null;
            }
        }

        @Override
        public synchronized void close() throws IOException {
            this.closePipe();
            this.reader.close();
        }
    }
}
