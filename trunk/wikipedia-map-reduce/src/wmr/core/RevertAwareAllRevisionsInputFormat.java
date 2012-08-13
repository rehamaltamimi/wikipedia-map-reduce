/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package wmr.core;

import gnu.trove.map.TLongLongMap;
import gnu.trove.map.hash.TLongLongHashMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import wmr.core.AllRevisionsInputFormat.WPRecordReader.RevisionIterable;

/**
 *
 * @author shilad
 */
public class RevertAwareAllRevisionsInputFormat extends FileInputFormat<Long, AllRevisions> {
    private static final Logger LOG = Logger.getLogger(RevertAwareAllRevisionsInputFormat.class.getName());
    
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return true;
    }

    @Override
    public RecordReader<Long, AllRevisions> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        RevertReader reader = new RevertReader();
        reader.initialize(split, context);
        return reader;
    }

    /**
     * Most of this code borrowed from LineRecordReader
     */
    public static class RevertReader extends RecordReader<Long, AllRevisions> {
        private AllRevisionsInputFormat.WPRecordReader delegate = new AllRevisionsInputFormat.WPRecordReader();

        private FileSplit split;
        private TaskAttemptContext context;
        private AllRevisions value;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            this.split = (FileSplit) split;
            assert(this.split.getStart() == 0);
            this.context = context;
            this.delegate.initialize(split, context);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (!delegate.nextKeyValue()) {
                return false;
            }
            AllRevisions delegateValue = delegate.getCurrentValue();
            value = new AllRevisions(
                    delegateValue.getPage(),
                    new RevertIterable(context, (RevisionIterable) delegateValue.getRevisions()));
            return true;
        }

        private static class RevertIterable implements Iterable<Revision>, Iterator<Revision> {
            private static final int LOOKAHEAD_WINDOW = 10;
            
            private List<Revision> queue = new ArrayList<Revision>();
            private final TaskAttemptContext context;
            private final RevisionIterable delegate;
            TLongLongMap fingerprints = new TLongLongHashMap();

            public RevertIterable(TaskAttemptContext context, RevisionIterable delegate) {
                this.delegate = delegate;
                this.context = context;
            }
            public Iterator<Revision> iterator() {
                return this;
            }
            public boolean hasNext() {
                return (delegate.hasNext() || !queue.isEmpty());
            }
            public Revision next() {
                // fill the queue
                while (delegate.hasNext() && queue.size() < LOOKAHEAD_WINDOW) {
                    Revision r = delegate.next();
                    queue.add(r);
                }

                Revision head = queue.remove(0);
                Long fp = head.getTextFingerprint();
                
                // if the revision is long enough and we haven't seen the text before, remember it
                if (head.getText().trim().length() > 10 && fingerprints.containsKey(fp)) {
                    head.setRevert(true);   // reverting revision
                }

                // look for a strict vandalism revert in the next edit
                if (queue.size() > 0 && queue.get(0).isVandalismStrictRevert()) {
                    head.setVandalism(true);
                }
                
                // look for a subsequent reverting revision
                // TODO: limit the fingerprints to a certain number of past edits
                for (Revision r : queue) {
                    if (fingerprints.containsKey(r.getTextFingerprint())) {
                        head.setReverted(true);
                        break;
                    }
                }

                if (head.getText().trim().length() > 10 && !fingerprints.containsKey(fp)) {
                    fingerprints.put(fp, Long.valueOf(head.getId()));
                }
                
                return head;
            }
            public void remove() {
                throw new UnsupportedOperationException("Not supported.");
            }
        }

        @Override
        public Long getCurrentKey() throws IOException, InterruptedException {
            return delegate.getCurrentKey();
        }

        @Override
        public AllRevisions getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return delegate.getProgress();
        }
        @Override
        public synchronized void close() throws IOException {
            delegate.close();
        }
    }
}
