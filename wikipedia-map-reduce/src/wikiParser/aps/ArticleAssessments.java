/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wikiParser.aps;

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
import wikiParser.*;
import wikiParser.mapReduce.util.*;
import wikiParser.util.*;

/**
 *
 * @author Shilad Sen
 * 
 */
public class ArticleAssessments extends Configured implements Tool {

    /*
     * Takes key-value 7zip hashes and outputs url-id pairs.
     */
    public static class MyMapper extends Mapper<Text, Text, Text, Text> {

        public void map(Text key, Text value, Mapper.Context context) throws IOException {
            LzmaPipe pipe = null;
            try {
                context.progress();
                int length = MapReduceUtils.unescapeInPlace(value.getBytes(), value.getLength());
                pipe = new LzmaPipe(value.getBytes(), length);
                PageParser parser = new PageParser(pipe.decompress());
                Page article = parser.getArticle();
                if (article.isTalk()) {
                    System.err.println("processing article " + key + "(" + parser.getArticle().getName() + ")");
                    Map <Assessment, Integer> counts = new HashMap<Assessment, Integer>();
                    while (true) {
                        context.progress();
                        Revision rev = parser.getNextRevision();
                        if (rev == null) {
                            break;
                        }
                        //System.err.println("doing revision " + rev.getId() + " at " + rev.getTimestamp());
                        Map<Assessment, Integer> newCounts = processRevision(context, parser.getArticle(), rev);
                        Set<Assessment> all = new HashSet<Assessment>(counts.keySet());
                        all.addAll(newCounts.keySet());
                        for (Assessment a : all) {
                            int c0 = counts.containsKey(a) ? counts.get(a) : 0;
                            int c1 = newCounts.containsKey(a) ? newCounts.get(a) : 0;
                            if (c0 != c1) {
                            context.write(
                                    new Text(article.getName() + "@" + article.getId()),
                                    new Text(
                                            rev.getTimestamp() + "\t" +
                                            c0 + "\t" +
                                            c1 + "\t" +
                                            a.getTemplateName() + "\t" +
                                            a.getAssessment() + "\t" +
                                            a.getImportance() + "\t"
                                        )
                                );
                            }
                        }
                        counts = newCounts;
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

        private Map<Assessment, Integer> processRevision(Mapper.Context context, Page page, Revision rev) throws IOException {
            Map<Assessment, Integer> counts = new HashMap<Assessment, Integer>();
            for (Template t : rev.getTemplates()) {
                for (Assessment a : Assessment.templateToAssessment(page, rev, t)) {
                    a.setTimestamp("");     // ignore timestamp; may have been added a long time ago.
                    Integer c = counts.get(a);
                    counts.put(a, (c == null) ? 1 : (c+1));
                }
            }
            return counts;
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

        
        job.setJarByClass(ArticleAssessments.class);
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
        int res = ToolRunner.run(new ArticleAssessments(), args);
        System.exit(res);
    }
}
