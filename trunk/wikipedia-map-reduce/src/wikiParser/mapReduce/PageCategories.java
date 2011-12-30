/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wikiParser.mapReduce;

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
import wikiParser.*;

/**
 * @author Shilad Sen
 */
public class PageCategories extends Configured implements Tool {

    public static class MyMapper extends Mapper<Long, CurrentRevision, Text, Text> {

        @Override
        public void map(Long key, CurrentRevision value, Mapper.Context context)
                throws IOException, InterruptedException {
            try {
                context.progress();
                Page p = value.getPage();
                if (p.isNormalPage() || p.isCategory()) {
                    Revision r = value.getRevision();
                    StringBuilder cats = new StringBuilder();
                    for (String link : r.getAnchorLinksWithoutFragments()) {
                        if (link.startsWith("Category:")) {
                            if (cats.length() > 0) {
                                cats.append("\t");
                            }
                            cats.append(link);
                        }
                    }
                    String code = p.isCategory() ? "c" : "p";
                    context.write(
                            new Text(""+key),
                            new Text(code + "\t" + p.getName() + "\t" + cats.toString()));
                }
                context.progress();
            } catch (Exception e) {
                System.err.println("processing of " + key + " failed:");
                e.printStackTrace();
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

        
        job.setJarByClass(PageCategories.class);
        job.setInputFormatClass(CurrentRevisionInputFormat.class);
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
        int res = ToolRunner.run(new PageCategories(), args);
        System.exit(res);
    }
}
