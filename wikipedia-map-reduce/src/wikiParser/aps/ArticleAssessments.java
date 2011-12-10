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
    public static class AssessmentCount {
        private Assessment assessment;
        private int count;

        public AssessmentCount(Assessment a) {
            this.assessment = a;
            this.count = 1;
        }

        public void increment() {
            this.count++;
        }
    };

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
                context.progress();
                if (article.isTalk()) {
                    System.err.println("processing article " + key + "(" + parser.getArticle().getName() + ")");
                    Map <String, AssessmentCount> counts = new HashMap<String, AssessmentCount>();
                    while (true) {
                        context.progress();
                        Revision rev = parser.getNextRevision();
                        if (rev == null) {
                            break;
                        }
                        //System.err.println("doing revision " + rev.getId() + " at " + rev.getTimestamp());
                        Map<String, AssessmentCount> newCounts = processRevision(context, parser.getArticle(), rev);

                        // added assessments
                        for (String ak : newCounts.keySet()) {
                            AssessmentCount acNew = newCounts.get(ak);
                            Assessment a = acNew.assessment;
                            AssessmentCount acPrev = counts.get(ak);
                            int c0 = acPrev == null ? 0 : acPrev.count;
                            int c1 = acNew.count;
                            if (c0 != c1) {
                                context.write(
                                    new Text(article.getName() + "@" + article.getId()),
                                    new Text(
                                            rev.getTimestamp() + "\t" +
                                            rev.getId() + "\t" +
                                            a.getUser().getName() + "@" + a.getUser().getId() + "\t" +
                                            a.isFromBot() + "\t" +
                                            c0 + "\t" +
                                            c1 + "\t" +
                                            a.getTemplateName() + "\t" +
                                            a.getAssessment() + "\t" +
                                            a.getImportance() + "\t"
                                        )
                                );
                            }
                        }

                        // deleted assessments
                        for (String ak : counts.keySet()) {
                            AssessmentCount acNew = newCounts.get(ak);
                            AssessmentCount acPrev = counts.get(ak);
                            Assessment a = acPrev.assessment;
                            int c0 = acPrev.count;
                            int c1 = acNew == null ? 0 : acNew.count;
                            if (c0 != c1) {
                                context.write(
                                    new Text(article.getName() + "@" + article.getId()),
                                    new Text(
                                            rev.getTimestamp() + "\t" +
                                            rev.getId() + "\t" +
                                            a.getUser().getName() + "@" + a.getUser().getId() + "\t" +
                                            a.isFromBot() + "\t" +
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

        private Map<String, AssessmentCount> processRevision(Mapper.Context context, Page page, Revision rev) throws IOException {
            Map<String, AssessmentCount> counts = new HashMap<String, AssessmentCount>();
            for (Template t : rev.getTemplates()) {
                for (Assessment a : Assessment.templateToAssessment(page, rev, t)) {
                    String k = a.getSemanticKey();
                    AssessmentCount c = counts.get(k);
                    if (c == null) {
                        counts.put(k, new AssessmentCount(a));
                    } else {
                        c.increment();
                    }
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
