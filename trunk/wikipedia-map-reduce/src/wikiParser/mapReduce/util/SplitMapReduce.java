
package wikiParser.mapReduce.util;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
/**
 * @author Shilad
 *
 * Splits the wikipedia input file into smaller input files.
 * We must be careful in the output from the map because the values outputted
 * are file lines.
 *
 * This inserts null bytes somewhere strange.  I can't tell where.
 */
public class SplitMapReduce {

    public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, BytesWritable> {
        private static final int NUM_DIGITS = 2;
        public void map(Text key, Text value, OutputCollector<Text, BytesWritable> output,
                Reporter reporter) throws IOException {

            // Constructs the key
            long id = MapReduceUtils.keyToId(key);
            if (id < 0) {
                System.err.println("invalid key: " + key);
                return;
            }
            String idStr = "" + id;
            while (idStr.length() < NUM_DIGITS) {
                idStr = "0" + idStr;
            }
            String newKey = idStr.substring(idStr.length() - NUM_DIGITS);

            // Constructs the value.
            byte newValue[] = new byte[key.getLength() + 1 + value.getLength()];
            int i = 0;
            for (int j = 0; j < key.getLength(); j++) {
                newValue[i++] = key.getBytes()[j];
            }
            newValue[i++] = '\t';
            for (int j = 0; j < value.getLength(); j++) {
                newValue[i++] = value.getBytes()[j];
            }
            assert(i == newValue.length);            

            output.collect(new Text(newKey), new BytesWritable(newValue));
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, BytesWritable, Text, Text> {

        private static final Path OUTPUT_PATH = new Path("s3n://macalester/data");

        FSDataOutputStream out;
        JobConf conf;

        @Override
        public void configure(JobConf conf) {
            this.conf = conf;
        }

        public void reduce(Text key, Iterator<BytesWritable> values, OutputCollector<Text, Text> output,
                Reporter reporter) throws IOException {
            openFile(key);
            try {
                while (values.hasNext()) {
                    BytesWritable value = values.next();
                    out.write(value.get(), 0, value.getSize());
                    out.writeChar('\n');
                }
            } finally {
                closeFile();
            }
        }

        private void openFile(Text key) throws IOException {
            if (out == null) {
                Path path = new Path(OUTPUT_PATH + "/wikipedia." + key.toString() + ".txt");
                System.out.println("opening " + path);
                FileSystem fs = FileSystem.get(path.toUri(), conf);
                if (fs.isFile(path)) {
                    fs.delete(path);
                }
                out = fs.create(path);
            }
        }

        private void closeFile() throws IOException {
            out.close();
            out = null;
        }
    }

    public static void main(String args[]) throws IOException {
        SimpleJobConf conf = new SimpleJobConf(Map.class, Reduce.class, MapReduceUtils.S3_INPUTS, "splits", "splitter");
        conf.getConf().setNumMapTasks(10);
        conf.getConf().setNumReduceTasks(10);
        conf.getConf().setMapOutputValueClass(BytesWritable.class);
        conf.run();
    }
}
