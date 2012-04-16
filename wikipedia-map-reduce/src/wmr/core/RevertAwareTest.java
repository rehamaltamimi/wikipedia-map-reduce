/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wmr.core;

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

/**
 *
 * @author Shilad Sen
 * 
 * Test for the revert aware revision format.
 */
public class RevertAwareTest extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(RevertAwareTest.class.getName());

    public static class MyMapper extends Mapper<Long, AllRevisions, Text, Text> {
        
        @Override
        public void setup(Mapper.Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        public void map(Long pageId, AllRevisions revs, Context context) throws IOException, InterruptedException {
            try {
                Page page = revs.getPage();
                context.progress();
                for (Revision r : revs.getRevisions()) {
                    context.progress();
                    try {
                        String cats = "";
                        if (r.isVandalism()) {
                            cats += "vandalism ";
                        }
                        if (r.isReverted()) {
                            cats += "reverted ";
                        }
                        if (r.isRevert()) {
                            cats += "revert ";
                        }
                        if (cats.length() == 0) {
                            cats = "clean";
                        }
                        System.out.println("" + page.getName() + " @ " + r.getTimestamp() + ": " + cats);
                    } catch (Exception e) {
                        LOG.log(Level.SEVERE, "Error processing page " + pageId + ", rev " + r.getId(), e);
                    }
                }
            } catch (Exception e) {
                LOG.log(Level.SEVERE, "Error processing page " + pageId, e);
            }
        }
    }

    public int run(String args[]) throws Exception {

        if (args.length < 2) {
            System.out.println("usage: [input output]");
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        Configuration conf = getConf();
        Job job = new Job(conf, this.getClass().toString());

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        
        job.setJarByClass(RevertAwareTest.class);
        job.setInputFormatClass(RevertAwareAllRevisionsInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(Reducer.class); // identity reducer
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
        int res = ToolRunner.run(new RevertAwareTest(), args);
        System.exit(res);
    }
}
