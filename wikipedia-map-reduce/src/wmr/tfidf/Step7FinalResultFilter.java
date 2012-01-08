/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wmr.tfidf;

import gnu.trove.set.hash.TIntHashSet;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import wikiParser.mapReduce.util.KeyValueTextInputFormat;

import wmr.util.EasyLineReader;

/**
 * @author Shilad Sen
 */
public class Step7FinalResultFilter extends Configured implements Tool {

    private static final Logger LOG  = Logger.getLogger(Step7FinalResultFilter.class.getPackage().getName());
    private static final String KEY_ID_FILTER = "ID_FILTER";

    public static final int MIN_DOCUMENT_LENGTH = 10;

    public static class MyMapper extends Mapper<Text, Text, Text, Text> {

        TIntHashSet filter = null;

        @Override
        public void setup(Mapper.Context context) throws IOException {
            String path = context.getConfiguration().get(KEY_ID_FILTER);
            assert(path != null);
            LOG.log(Level.INFO, "filter path was {0}", path);
            filter = new TIntHashSet();
            EasyLineReader reader = new EasyLineReader(new Path(path), context.getConfiguration());
            for (String line : reader) {
                try {
                    filter.add(Integer.valueOf(line.split("\\s+")[0]));
                } catch (NumberFormatException e) {
                    // ignore it....
                }
            }
            LOG.log(Level.INFO, "Read {0} pages into filter", filter.size());
        }

        @Override
        public void map(Text key, Text value, Mapper.Context context)
                throws IOException, InterruptedException {
            try {
                context.progress();
                int pageId = Integer.valueOf(key.toString());
                if (filter.contains(pageId)) {
                    context.write(key, value);
                }
            } catch (Exception e) {
                System.err.println("processing of " + key + " failed:");
                e.printStackTrace();
            }
        }
    }


    public int run(String args[]) throws Exception {

        if (args.length < 3) {
            System.out.println("usage: input output id_filter");
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        Configuration conf = getConf();
        conf.set(KEY_ID_FILTER, args[2]);
        
        Job job = new Job(conf, this.getClass().toString());

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        
        job.setJarByClass(Step7FinalResultFilter.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setMapperClass(MyMapper.class);
        FileSystem hdfs = FileSystem.get(outputPath.toUri(), conf);
        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Dispatches command-line arguments to the tool via the
     * <code>ToolRunner</code>.
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Step7FinalResultFilter(), args);
        System.exit(res);
    }
}
