/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wmr.health;


import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.array.TLongArrayList;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

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
import org.wikipedia.miner.util.MarkupStripper;

import wmr.core.*;

/**
 *
 * @author Shilad Sen
 * 
 */
public class RevisionTextGenerator extends Configured implements Tool {
	public static List<Long> DESIRED_PAGES = Arrays.asList(2899107L, 214148L, 12109814L);

    public static class MyMapper extends Mapper<Long, AllRevisions, Text, Text> {

        /**
         * Outputs user, timestamp pairs for every main namespace edit.
         */
        @Override
        public void map(Long pageId, AllRevisions revs, Mapper.Context context) throws IOException, InterruptedException {
            Page article = revs.getPage();
            context.progress();
            
            if (article.isNormalPage()) {
//            	describeRevisions(pageId, revs, context, article);
            	writeRevisions(pageId, revs, context, article);
            }
        }
        

		private void writeRevisions(Long pageId, AllRevisions revs, Mapper.Context context, Page article) throws IOException, InterruptedException {
		    context.progress();
			if (!DESIRED_PAGES.contains(pageId)) {
				return;
			}
			for (Revision rev : revs.getRevisions()) {
			    context.progress();
				String cleanedText = MarkupStripper.stripEverything(rev.getText()).replaceAll("\\s+", " ");
			    User u = rev.getContributor();
			    String uname = (u.getName() == null || u.getName().trim().length() == 0) ? u.getId() : u.getName();
			    context.write(
			    		new Text(article.getName()), 
			    		new Text(
			    				rev.getId() + "\t" +
			    				rev.getTimestamp() + "\t" +
	    						uname + "\t" +
			    				cleanedText
			    		)
		    		);
			}
		}
		
		private void describeRevisions(Long pageId, AllRevisions revs, Mapper.Context context, Page article) throws IOException, InterruptedException {
			long sum = 0;
			TIntArrayList bytes = new TIntArrayList();
			for (Revision rev : revs.getRevisions()) {
			    context.progress();
			    if (rev == null) {
			        continue;
			    }
			    bytes.add(rev.getText().length());
			    sum += rev.getText().length(); 
			}
			bytes.sort();
			double mean = bytes.isEmpty() ? Double.NaN : 1.0 * sum / bytes.size();
			int median = bytes.isEmpty() ? 0 : bytes.get(bytes.size() / 2);
			context.write(new Text(""+pageId), 
					new Text(article.getName() + "\t" + bytes.size() + "\t" + median + "\t" + mean));
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

        
        job.setJarByClass(RevisionTextGenerator.class);
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
        int res = ToolRunner.run(new RevisionTextGenerator(), args);
        System.exit(res);
    }
}
