/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wikiParser.mapReduce.graphs;


import java.io.IOException;
import java.util.*;
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
import wikiParser.mapReduce.util.*;
import wmr.util.*;

/**
 *
 * @author Shilad Sen
 * 
 */
public class RevertGraphGenerator extends Configured implements Tool {

    /*
     * Takes key-value 7zip hashes and outputs url-id pairs.
     */
    public static class MyMapper extends Mapper<Text, Text, Text, Text> {

        public void map(Text key, Text value, Mapper.Context context) throws IOException {
            LzmaDecompresser pipe = null;
            try {
                context.progress();
                int length = MapReduceUtils.unescapeInPlace(value.getBytes(), value.getLength());
                pipe = new LzmaDecompresser(value.getBytes(), length);
                PageParser parser = new PageParser(pipe.decompress());
                Page article = parser.getArticle();
                User lastEditor = null;
                context.progress();
                if (article.isNormalPage()) {
                    System.err.println("processing article " + key + "(" + article.getName() + ")");
                    Set<Long> fingerprints = new HashSet<Long>();
                    while (true) {
                        context.progress();
                        Revision rev = parser.getNextRevision();
                        if (rev == null) {
                            break;
                        }

                        Long fingerprint = rev.getTextFingerprint();
                        String code = null;
                        if (rev.isVandalismRevert()) {
                            code = "cv";
                        } else if (rev.isRevert()) {
                            code = "cr";
                        } else if (fingerprints.contains(fingerprint)) {
                            code = "fr";
                        }
                        if (code != null && lastEditor != null) {
                            context.write(
                                    new Text(key + "@" + article.getName()),
                                    new Text(
                                        rev.getId() + "\t" +
                                        rev.getTimestamp() + "\t" +
                                        code + "\t" +
                                        lastEditor.getId() + "@" + lastEditor.getName() + "\t" +
                                        rev.getContributor().getId() + "@" + rev.getContributor().getName())
                                );
//                            System.err.println(
//                                        article.getName() + "\t" +
//                                        rev.getId() + "\t" +
//                                        rev.getTimestamp() + "\t" +
//                                        code + "\t" +
//                                        lastEditor.getId() + "@" + lastEditor.getName() + "\t" +
//                                        rev.getContributor().getId() + "@" + rev.getContributor().getName()
//                                    );
                        }
                        fingerprints.add(fingerprint);
                        lastEditor = rev.getContributor();
                    }
                }
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

        
        job.setJarByClass(RevertGraphGenerator.class);
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
        int res = ToolRunner.run(new RevertGraphGenerator(), args);
        System.exit(res);
    }
}
