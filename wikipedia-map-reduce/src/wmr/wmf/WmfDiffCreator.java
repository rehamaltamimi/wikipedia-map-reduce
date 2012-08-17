/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package wmr.wmf;

import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import wmr.core.AllRevisionsInputFormat;
import wmr.util.SecondarySortOnSpace;

/**
 *
 * @author shilad
 */
public class WmfDiffCreator extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(WmfDiffCreator.class.getName());
    public static final String KEY_ID_FILTER = "ID_FILTER";


    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("usage: input output {id_filter_path}");
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        Configuration conf = getConf();
        if (args.length >= 3) {
            conf.set(KEY_ID_FILTER, args[2]);
        }
        
        Job job = new Job(conf, this.getClass().toString());

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        SecondarySortOnSpace.setupSecondarySortOnSpace(job);

        job.setJarByClass(WmfDiffCreator.class);
        job.setInputFormatClass(AllRevisionsInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(WmfDiffMapper.class);
        job.setReducerClass(WmfDiffReducer.class); // identity reducer

        FileSystem hdfs = FileSystem.get(outputPath.toUri(), conf);
        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new WmfDiffCreator(), args);
        System.exit(res);
    }
}
