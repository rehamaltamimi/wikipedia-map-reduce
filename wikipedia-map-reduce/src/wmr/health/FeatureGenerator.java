/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wmr.health;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
import org.wikipedia.miner.util.FleschIndex;
import org.wikipedia.miner.util.MarkupStripper;
import wmr.core.*;
import wmr.templates.Template;

/**
 *
 * @author Shilad Sen
 * 
 */
public class FeatureGenerator extends Configured implements Tool {

    public static class MyMapper extends Mapper<Long, AllRevisions, Text, Text> {

        /**
         * Compute salient visual features for each revision.
         */
        @Override
        public void map(Long pageId, AllRevisions revs, Mapper.Context context) throws IOException, InterruptedException {
            Page article = revs.getPage();
            context.progress();
            if (!article.isNormalPage() || article.isAnyTalk()) {
                return;
            }
            
            for (Revision rev : revs.getRevisions()) {
               context.progress();
               if (rev == null) {
                    continue;
               }
               String text = MarkupStripper.stripEverything(rev.getText());
               int numWikilinks = rev.getAnchorLinks().size();
               int numHyperlinks = rev.getHyperlinks().size();
               int bytes = text.length();
               int words = text.split("\\W").length;
               int sections = rev.getSections().size();
               int citations = rev.getCitations(article).size();
               float flesch = FleschIndex.calculate(text);

               // Flatten nested templates up to two levels deep
               List<Template> templates = new ArrayList<Template>();
               for (Template t : rev.getTemplates()) {
                   templates.add(t);
                   for (String param : t.getAllParams().keySet()) {
                       if (t.paramContainsTemplate(param)) {
                           templates.addAll(t.getParamAsTemplate(param));
                       }
                   }
               }

               int numBoxes = 0;
               for (Template t : templates) {
                   if (t.getName().toLowerCase().startsWith("infobox")) {
                       numBoxes++;
                   }
               }
               int numFiles = rev.getFileLinks().size();

               context.write(
                   new Text("" + rev.getId()),
                   new Text( "" +
                       numWikilinks + "\t" +
                       numHyperlinks + "\t" +
                       bytes + "\t" +
                       words + "\t" +
                       citations + "\t" + 
                       sections + "\t" +
                       flesch + "\t" +
                       numBoxes + "\t" +
                       numFiles + "\t"
                   ));
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

        
        job.setJarByClass(FeatureGenerator.class);
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
        int res = ToolRunner.run(new FeatureGenerator(), args);
        System.exit(res);
    }
}
