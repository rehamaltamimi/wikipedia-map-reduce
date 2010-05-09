package wikiParser.mapReduce.metrics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;

import wikiParser.Page;
import wikiParser.PageParser;
import wikiParser.RevisionFingerprinter;
import wikiParser.Revision;
import wikiParser.User;
import wikiParser.mapReduce.statistics.RevisionCountData;
import wikiParser.mapReduce.util.MapReduceUtils;

public class NeighboringEditorsMapReduce {

	public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, OutputCollector<Text, Text> output, 
				Reporter reporter) throws IOException {
			try {
				RevisionFingerprinter fp = new RevisionFingerprinter();
				PageParser parser = MapReduceUtils.formatArticleParser(value);
				Page article = parser.getArticle();
				Revision rev = null;;
				if (article.isUserTalk() || article.isTalk()) {
					rev = parser.getNextRevision();
					while (rev != null) {
						ArrayList<String> uniqueText = fp.findUniqueText(rev);
						Set<User> neighbors = fp.neighboringContributors(rev, uniqueText);
						for (User neighbor : neighbors) {
							output.collect(new Text(rev.getContributor().getId()), new Text("t"+neighbor.toUnderscoredString()));
						}
						rev = parser.getNextRevision();
					}
				} else if (article.isUser()) {
					output.collect(new Text("count"), new Text("1"));
				} else {
					rev = parser.getNextRevision();
					while (rev != null) {
						output.collect(new Text(article.getId()), new Text("c"+rev.getContributor().toUnderscoredString()));
						rev = parser.getNextRevision();
					}
				}
			} catch (Exception e) {
				System.err.println("error when processing " + key + ":");
				e.printStackTrace();
			}
			/*
			 * 
			 * If article is Talk: or User talk:
			 * Step 1: read in revisions from an article (PageParser)
			 * Step 2: find the unique text for each user (RevisionFingerprinter.findUniqueText())
			 * Step 3: determine the owners of the nearby text (RevisionFingerprinter.neighboringContributors())
			 * Step 4: emit user-user "talking" pairs
			 * 
			 * If article is not Talk: or User talk:
			 * Step 1: read in revisions from an article (PageParser)
			 * Step 2: get a list of all contributors
			 * Step 3: emit user-user "co-author" pairs
			 * 
			 * Final movement (non-M/R):
			 * Step 1: read in both end-files
			 * Step 2: For each user, determine the probability that they co-author and talk,
			 * 			and co-author and not talk. (Are these two variables dependent?) ::Correlation?? ok, i'm not crazy
			 * P(ca&talk): #ca&talk/#ca
			 * P(ca&!talk): #ca&!talk/#ca
			 * What effect does co-authoring have on talking?
			 */
		}
	}

	public class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text,Text> output, 
				Reporter reporter) throws IOException {
			String result = "";
			int sum = 0;
			if (key.toString().equals("count")) {
				while (values.hasNext()) {
					sum++;
				}
				output.collect(key, new Text(Integer.toString(sum)));
			}
			else {
				while (values.hasNext()) {
					result += (values.next().toString() + " ");
				}
				output.collect(key, new Text(result));
			}
		}
	}

	public static void runMe(String inputFile, String outputDir, String jobName) throws IOException{
		JobConf conf = new JobConf(RevisionCountData.class);
		conf.setJobName(jobName);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(KeyValueTextInputFormat.class);  // input to mapper
		conf.setOutputFormat(TextOutputFormat.class);  //output file from reducer

		FileSystem hdfs = FileSystem.get(conf);
		Path inputPath = new Path(inputFile);
		Path outputPath = new Path(outputDir);
		if (hdfs.isDirectory(outputPath))
			hdfs.delete(new Path(outputDir), true);

		FileInputFormat.setInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, outputPath);

		conf.setNumMapTasks(100);
		conf.setNumReduceTasks(1);
		conf.setMaxMapTaskFailuresPercent(50);
		conf.setMaxReduceTaskFailuresPercent(50);
		conf.setMaxMapAttempts(2);

		JobClient.runJob(conf);
	}
}
