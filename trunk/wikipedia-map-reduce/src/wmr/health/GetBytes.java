/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wmr.health;


import java.io.IOException;

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
import wmr.core.*;


/**
 *
 * @author Shilad Sen
 * 
 */
public class GetBytes extends Configured implements Tool {

    public static class MyMapper extends Mapper<Long, AllRevisions, Text, Text> {

        /**
         * Outputs ...
         */
        @Override
        public void map(Long pageId, AllRevisions revs, Mapper.Context context) throws IOException, InterruptedException {
            Page article = revs.getPage();
            context.progress();
            
            for (Revision rev : revs.getRevisions()) {
               context.progress();
               if (rev == null) {
                    continue;
               }
               User u = rev.getContributor();
               if (!u.isBot() && !u.isAnonymous()) {
                  String key = u.getName();
                  String namespace = "" + article.getNamespace();  
                  String val = "" + rev.getTimestamp();
                  
                  String text = rev.getText();
                  byte[] bytes = text.getBytes("UTF_8");
                  int numbytes = bytes.length;
                                 
                  String pair = val + "\t" + namespace;
                  context.write(new Text(key), new Text(pair));
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

        
        job.setJarByClass(GetBytes.class);
        job.setInputFormatClass(AllRevisionsInputFormat.class);
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
        int res = ToolRunner.run(new GetBytes(), args);
        System.exit(res);
    }
}
