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
public class DisambiguationsAndRedirects extends Configured implements Tool {

    public static class MyMapper extends Mapper<Long, CurrentRevision, Text, Text> {

        @Override
        public void map(Long key, CurrentRevision value, Mapper.Context context)
                throws IOException, InterruptedException {
            try {
                context.progress();
                Page p = value.getPage();
                if (p.isNormalPage()) {
                    Revision r = value.getRevision();
                    if (r.isRedirect()) {
                        context.write(
                                new Text("r@" + key + "@" + p.getName()),
                                new Text(r.getRedirectDestination()));
                    } else if (r.isDisambiguation()) {
                        StringBuilder links = new StringBuilder();
                        for (String link : r.getAnchorLinksWithoutFragments()) {
                              // ignore categories and inter-language links like "en:Foo"
                            if (link.indexOf(":") == 2 || link.startsWith("Category:")) {
                                continue;
                            }
                            if (links.length() > 0) {
                                links.append("\t");
                            }
                            links.append(link);
                        }
                        context.write(
                                new Text("d@" + key + "@" + p.getName()),
                                new Text(links.toString()));

                    }
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

        
        job.setJarByClass(DisambiguationsAndRedirects.class);
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
        int res = ToolRunner.run(new DisambiguationsAndRedirects(), args);
        System.exit(res);
    }
}
