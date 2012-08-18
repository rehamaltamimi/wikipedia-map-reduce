/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wmr.core;

import java.io.BufferedInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import wikiParser.mapReduce.util.KeyValueTextInputFormat;
import wmr.util.LzmaDecompresser;
import wmr.util.Utils;

/**
 *
 * @author Shilad Sen
 * Decompresses history of articles to an output directory, one article per file.
 */
public class DecompressingMapper extends Configured implements Tool {

    /*
     * Takes key-value 7zip hashes and outputs article name / id pairs
     */
    public static class MyMapper extends Mapper<Text, Text, Text, Text> {

        @Override
        public void map(Text key, Text value, Mapper.Context context) throws IOException {
            LzmaDecompresser pipe = null;
            Path outPath = FileOutputFormat.getOutputPath(context);
            FileSystem fs =FileSystem.get(context.getConfiguration()); 
            FSDataOutputStream out = fs.create(new Path(outPath, key.toString())); 
            try {
                context.progress();
                int length = Utils.unescapeInPlace(value.getBytes(), value.getLength());
                pipe = new LzmaDecompresser(value.getBytes(), length);
                BufferedInputStream in = new BufferedInputStream(pipe.decompress());
                byte buffer[] = new byte[512000];
                while (true) {
                	int i = in.read(buffer);
                	if (i < 0) {
                		break;
                	}
                	out.write(buffer, 0, i);
                }
                in.close();
            } catch (Exception e) {
                context.progress();
                System.err.println("error when processing " + key + ":");
                e.printStackTrace();
            } finally {
            	out.close();
                if (pipe != null) {
                    pipe.cleanup();
                }
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

        
        job.setJarByClass(DecompressingMapper.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
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
        int res = ToolRunner.run(new DecompressingMapper(), args);
        System.exit(res);
    }
}
