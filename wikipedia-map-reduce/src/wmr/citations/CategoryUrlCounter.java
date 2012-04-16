/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wmr.citations;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
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
 * First stage of citation counting process
 * Output: 
 * K: link@articleId, V: #added  #removed    #revisions
 */
public class CategoryUrlCounter extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(CategoryUrlCounter.class.getName());

    public static class MyMapper extends Mapper<Long, AllRevisions, Text, Text> {
        @Override
        public void setup(Mapper.Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        public void map(Long pageId, AllRevisions revs, Context context) throws IOException, InterruptedException {
            try {
                Page page = revs.getPage();
                context.progress();
                Map<String, Integer> citeCounts = new HashMap<String, Integer>();
                for (Revision r : revs.getRevisions()) {
                    context.progress();
                    try {
                        if (r.isRevert() || r.isReverted() || r.isVandalism()) {
                            continue;
                        }
                        //System.err.println("doing revision " + rev.getId() + " at " + rev.getTimestamp());
                        Map<String, Integer> newCiteCounts = processRevision(page, r);
                        writeDiff(context, page, r, citeCounts, newCiteCounts);
                        citeCounts = newCiteCounts;
                    } catch (Exception e) {
                        LOG.log(Level.SEVERE, "Error processing page " + pageId + ", rev " + r.getId(), e);
                    }
                }
            } catch (Exception e) {
                LOG.log(Level.SEVERE, "Error processing page " + pageId, e);
            }
        }


        private void writeDiff(Mapper.Context context, Page article, Revision rev,
                Map<String, Integer> citeCounts, Map<String, Integer> newCiteCounts) throws IOException, InterruptedException {

            // deleted assessments
            for (String url : citeCounts.keySet()) {
                if (newCiteCounts.containsKey(url)) {
                    continue;   // will be listed later.
                }
                User u = rev.getContributor();
                int c0 = citeCounts.get(url);
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
                            url + "\t" +
                            c0 + "\t" +
                            c1 + "\t"
                        )
                    );
            }

            // added and updated assessments
            for (String url : newCiteCounts.keySet()) {
                User u = rev.getContributor();
                int c0 = citeCounts.containsKey(url) ? citeCounts.get(url) : 0;
                int c1 = newCiteCounts.get(url);
                if (c0 != c1) {
                    context.write(
                        new Text(article.getName() + "@" + article.getId()),
                        new Text(
                                rev.getTimestamp() + "\t" +
                                rev.getId() + "\t" +
                                u.getName() + "@" + u.getId() + "\t" +
                                u.isBot() + "\t" +
                                url + "\t" +
                                c0 + "\t" +
                                c1 + "\t"

                            )
                    );
                }
            }
        }

        private Map<String, Integer> processRevision(Page article, Revision rev) throws IOException {
            Map<String, Integer> sections = rev.getSections();
            HashMap<String, Integer> citeCounts = new HashMap<String, Integer>();
            for (Revision.Hyperlink link : rev.getHyperlinks()) {
                String section = getSection(link.getLocation(), sections);
                String url = section + "@" + link.getUrl().replaceAll("[\\s]+", " ");
                if (citeCounts.containsKey(url)) {
                    citeCounts.put(url, citeCounts.get(url) + 1);
                } else {
                    citeCounts.put(url, 1);
                }
            }
            return citeCounts;
        }

        private String getSection(int location, Map<String, Integer> sections) {
            String section = "header";
            int closest = 0;
            for (Entry<String, Integer> entry : sections.entrySet()) {
                if (entry.getValue() < location && entry.getValue() > closest) {
                    closest = entry.getValue();
                    section = entry.getKey();
                }
            }
            return section;
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

        
        job.setJarByClass(CategoryUrlCounter.class);
        job.setInputFormatClass(RevertAwareAllRevisionsInputFormat.class);
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
        int res = ToolRunner.run(new CategoryUrlCounter(), args);
        System.exit(res);
    }
}
