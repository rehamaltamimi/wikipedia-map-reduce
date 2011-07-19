/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wikiParser.mapReduce.statistics;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import wikiParser.*;
import wikiParser.mapReduce.util.*;
import wikiParser.util.*;

/**
 *
 * @author Nathaniel Miller
 * 
 * First stage of citation counting process
 * Output: 
 * K: url@articleId, V: #added  #removed    #revisions
 */
public class CitationCounter extends Configured implements Tool {

    /*
     * Takes key-value 7zip hashes and outputs url-id pairs.
     */
    public static class MyMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

        Map<String, Map<String,Integer>> citeCounts = new HashMap<String, Map<String,Integer>>();
        public void map(Text key, Text value, OutputCollector<Text, Text> output,
                Reporter reporter) throws IOException {
            
            LzmaPipe pipe = null;
            try {
                reporter.progress();
                int length = MapReduceUtils.unescapeInPlace(value.getBytes(), value.getLength());
                pipe = new LzmaPipe(value.getBytes(), length);
                PageParser parser = new PageParser(pipe.decompress());
                Page article = parser.getArticle();
                System.err.println("processing article " + key + "(" + parser.getArticle().getName() + ")");
                if (article.isNormalPage()) {//main namespace only
                    Set <String> urls = new HashSet<String>();
                    while (true) {
                        reporter.progress();
                        Revision rev = parser.getNextRevision();
                        reporter.progress();
                        if (rev == null) {
                            break;
                        }
//                        System.err.println("doing revision " + rev.getId() + " at " + rev.getTimestamp());
                        urls = processRevision(parser.getArticle(), rev, urls);
                    }
                    for (String url : citeCounts.keySet()) {
                        output.collect(new Text(url + "@" + article.getId()), new Text(citeCounts.get(url).get("added") +
                                "\t"+ citeCounts.get(url).get("removed") + "\t" + citeCounts.get(url).get("revisions")));
                    }
                    citeCounts.clear();
                }
            } catch (Exception e) {
                reporter.progress();
                System.err.println("error when processing " + key + ":");
                e.printStackTrace();
            } finally {
                if (pipe != null) {
                    pipe.cleanup();
                }
            }
        }

        private Set<String> processRevision(Page article, Revision rev, Set<String> prevUrls) throws IOException {
            HashSet<String> nextUrls = new HashSet<String>();
            for (Template t : rev.getCites()) {
                String url = t.getParam("url");
                if (url == null) {
                    url = t.getParam("templateName");
                    if (url == null) {
                        url = "NoURL";
                    }
                }
                url = url.replaceAll("[\\s]+", " ");
                nextUrls.add(url);
            }
            for (String cite : nextUrls) {
                if (citeCounts.get(cite) == null) {
                    Map citeInfo = new HashMap<String, Integer>();
                    citeInfo.put("added", 0);
                    citeInfo.put("revisions", 0);
                    citeInfo.put("removed", 0);
                    citeCounts.put(cite, citeInfo);
                }
                if (!prevUrls.contains(cite)) {
                    citeCounts.get(cite).put("added", citeCounts.get(cite).get("added") + 1);
                }
                citeCounts.get(cite).put("revisions", citeCounts.get(cite).get("revisions")  + 1);
            }
            for (String cite : prevUrls) {
                if (!nextUrls.contains(cite)) {
                    citeCounts.get(cite).put("removed", citeCounts.get(cite).get("removed") + 1);
                }
            }
//            if (Math.random() > 0.99) {
//                System.err.println("for revision " + rev.getTimestamp() + " found " + numPrinted + " of " + numUnique);
//            }
            return nextUrls;
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

        JobConf job = new JobConf(getConf(), this.getClass());
        job.setJobName(this.getClass().toString());

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setInputFormat(KeyValueTextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(IdentityReduce.class);
        FileSystem hdfs = FileSystem.get(outputPath.toUri(), job);
        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }

        JobClient.runJob(job);

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the
     * <code>ToolRunner</code>.
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CitationCounter(), args);
        System.exit(res);
        return;
    }
}
