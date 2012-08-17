package wmr.tstampsorter;

import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import wmr.util.SecondarySortOnSpace;

public class TstampSorterMain extends Configured implements Tool {
    static final Logger LOG = Logger.getLogger(TstampSorterMain.class.getName());
	
	// these values must be ordered alphabetically
	static final char KEY_LENGTH = '0';
	static final char KEY_HEADER = '1';
	static final char KEY_REVISION = '2';
	static final char KEY_FOOTER = '9';
	
	// Maximum number of bytes in the text of a revision 
	static int MAX_LENGTH = 500000;
	
    
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("usage: input output");
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        Configuration conf = getConf();
        Job job = new Job(conf, this.getClass().toString());

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        SecondarySortOnSpace.setupSecondarySortOnSpace(job);

        job.setJarByClass(TstampSorterMain.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(TstampSorterMapper.class);
        job.setReducerClass(TstampSorterReducer.class);

        FileSystem hdfs = FileSystem.get(outputPath.toUri(), conf);
        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new TstampSorterMain(), args);
        System.exit(res);
    }
}
