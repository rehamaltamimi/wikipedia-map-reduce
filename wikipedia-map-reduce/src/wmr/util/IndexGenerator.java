/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wmr.util;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import wmr.core.*;
import wikiParser.mapReduce.util.*;

/**
 *
 * @author Shilad Sen
 * 
 */
public class IndexGenerator extends Configured implements Tool {

    /*
     * Takes key-value 7zip hashes and outputs article name / id pairs
     */
    public static class MyMapper extends Mapper<Text, Text, Text, Text> {

        @Override
        public void map(Text key, Text value, Mapper.Context context) throws IOException {
            FileSplit split = (FileSplit) context.getInputSplit();
            String parts[] = split.getPath().toString().split("/");
            String path = parts.length == 0 ? "null" : parts[parts.length-1];

            LzmaDecompresser pipe = null;
            try {
                context.progress();
                int length = Utils.unescapeInPlace(value.getBytes(), value.getLength());
                pipe = new LzmaDecompresser(value.getBytes(), length);
                PageParser parser = new PageParser(pipe.decompress());
                Page article = parser.getArticle();
                context.write(
                        new Text(article.toUnderscoredString()),
                        new Text(article.getId() + "\t" + article.getNamespace() + "\t" + path)
                    );
            } catch (Exception e) {
                context.progress();
                System.err.println("error when processing " + key + ":");
                e.printStackTrace();
            } finally {
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

        
        job.setJarByClass(IndexGenerator.class);
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
        int res = ToolRunner.run(new IndexGenerator(), args);
        System.exit(res);
    }
}
