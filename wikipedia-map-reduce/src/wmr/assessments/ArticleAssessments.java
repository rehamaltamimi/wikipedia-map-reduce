/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wmr.assessments;

import wmr.templates.Template;
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
            LzmaDecompresser pipe = null;
            try {
                context.progress();
                int length = Utils.unescapeInPlace(value.getBytes(), value.getLength());
                pipe = new LzmaDecompresser(value.getBytes(), length);
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
                        Map<String, AssessmentCount> newCounts = processRevision(context, parser.getArticle(), rev, counts);

                        // deleted assessments
                        for (String ak : counts.keySet()) {
                            if (newCounts.containsKey(ak)) {
                                continue;   // will be listed later.
                            }
                            AssessmentCount acPrev = counts.get(ak);
                            Assessment a = acPrev.assessment;
                            User u = rev.getContributor();
                            int c0 = acPrev.count;
                            int c1 = 0;
                            if (c0 == 0) {
                                continue;   // shouldn't really happen
                            }
                            context.write(
                                new Text(article.getName() + "@" + article.getId()),
                                new Text(
                                        rev.getTimestamp() + "\t" +
                                        rev.getId() + "\t" +
                                        u.getName() + "@" + u.getId() + "\t" +
                                        u.isBot() + "\t" +
                                        c0 + "\t" +
                                        c1 + "\t" +
                                        a.getTemplateName() + "\t" +
                                        a.getAssessment() + "\t" +
                                        a.getImportance()
                                    )
                                );
                        }

                        // added and updated assessments
                        for (String ak : newCounts.keySet()) {
                            AssessmentCount acPrev = counts.get(ak);
                            AssessmentCount acNew = newCounts.get(ak);
                            Assessment a = acNew.assessment;
                            User u = rev.getContributor();
                            int c0 = acPrev == null ? 0 : acPrev.count;
                            int c1 = acNew.count;
                            if (c0 != c1) {
                                context.write(
                                    new Text(article.getName() + "@" + article.getId()),
                                    new Text(
                                            rev.getTimestamp() + "\t" +
                                            rev.getId() + "\t" +
                                            u.getName() + "@" + u.getId() + "\t" +
                                            (u.isBot() || a.isFromBot()) + "\t" +
                                            c0 + "\t" +
                                            c1 + "\t" +
                                            a.getTemplateName() + "\t" +
                                            a.getAssessment() + "\t" +
                                            a.getImportance()
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

        private Map<String, AssessmentCount> processRevision(
                Mapper.Context context, Page page,
                Revision rev, Map<String, AssessmentCount> oldCounts) throws IOException {
            Map<String, AssessmentCount> counts = new HashMap<String, AssessmentCount>();
            for (Template t : rev.getTemplates()) {
                for (Assessment a : Assessment.templateToAssessment(page, rev, t)) {
                    String k = a.getSemanticKey();
                    AssessmentCount c = counts.get(k);
                    if (c == null) {
                        AssessmentCount old = oldCounts.get(k);
                        if (old != null) {
                            a = old.assessment; // grab the original assessment
                        }
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
