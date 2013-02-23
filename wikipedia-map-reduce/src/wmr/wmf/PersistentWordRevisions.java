/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wmr.wmf;


import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;

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
import wmr.util.Utils;


/**
 *
 * @author Shilad Sen
 * @author Kyle Rosenberg
 * 
 */
public class PersistentWordRevisions extends Configured implements Tool {

    public static class MyMapper extends Mapper<Long, AllRevisions, Text, Text> {
        private Random rand = new Random();

        /**
         * Outputs the article id, revision id, editor name, PWRs.
         * PWR is the sum over all words w added in the revision(number of revisions w lasts).  
         */
        @Override
        public void map(Long pageId, AllRevisions revs, Mapper.Context context) throws IOException, InterruptedException {
            Page article = revs.getPage();
            context.progress();
            
            if (!article.isMain()) {
            	return;
            }
            
            TObjectIntHashMap<String> prevCounts = new TObjectIntHashMap<String>();
            
            int revIndex = 0;
            Map<String, TIntArrayList> wordInceptions = new HashMap<String, TIntArrayList>();
            TIntIntHashMap indexToRevId = new TIntIntHashMap();
            TIntObjectHashMap<String> indexToUser = new TIntObjectHashMap<String>();
            TIntIntHashMap pwrs = new TIntIntHashMap();
            
            for (Revision rev : revs.getRevisions()) {
               context.progress();
               if (rev == null) {
                    continue;
               }
               if (rev.isRevert() || rev.isReverted() || rev.isVandalism()) {
            	   continue;
               }
               User u = rev.getContributor();
               TObjectIntHashMap<String> nextCounts = getWordCounts(rev);
               
               indexToRevId.put(revIndex, Integer.valueOf(rev.getId()));
               indexToUser.put(revIndex,  (u.isBot() || u.isAnonymous()) ? null : u.getName());
               updatePwr(revIndex, getDiff(prevCounts, nextCounts), 
            		   wordInceptions, pwrs);
               revIndex++;
               prevCounts = nextCounts;
            }
            
            // remaining words in final revision, pretend they all survive one extra revision
           updatePwr(revIndex+1, getDiff(prevCounts, new TObjectIntHashMap<String>()), 
        		   wordInceptions, pwrs);
            
            for (int i = 0; i < revIndex; i++) {
            	if (pwrs.containsKey(i) && indexToUser.get(i) != null) {
	            	Text key = new Text(article.getName());
	            	Text value = new Text(
	            			indexToRevId.get(i) + "\t" +
	            			indexToUser.get(i) + "\t" +
	            			pwrs.get(i)
            			);
	            	context.write(key, value);
            	}
            }
        }

        /**
         * Updates the PWR counters for a given wordcount diff.
         * @param revIndex Index of the revision
         * @param diff Difference in word counts due to this revision
         * @param wordInceptions revision indexes in which a word was added.
         * @param pwrs Accumulator for pwr credits.
         */
		private void updatePwr(int revIndex, TObjectIntHashMap<String> diff,
				Map<String, TIntArrayList> wordInceptions, TIntIntHashMap pwrs) {
			for (String word : diff.keySet()) {
				   int delta = diff.get(word);
				   if (delta > 0) {
					   // added words
					   if (!wordInceptions.containsKey(word)) {
						   wordInceptions.put(word, new TIntArrayList(1));            			 
					   }
					   for (int i = 0; i < delta; i++) {
						   wordInceptions.get(word).add(revIndex);
					   }
				   } else {
					   // deleted words
					   for (int i = 0; i < -delta; i++) {
			    		   if (!wordInceptions.containsKey(word)) {
			    			   System.err.println("Word " + word + " not in wordIndexes");
			    			   continue;
			    		   }
			    		   TIntArrayList revIndexes = wordInceptions.get(word);
			    		   int j = rand.nextInt(revIndexes.size());
			    		   int ri = revIndexes.removeAt(j);
			    		   int survived = revIndex - ri - 1;	// -1 because first rev doesn't count
			    		   pwrs.adjustOrPutValue(ri, survived, survived);
			    		   if (revIndexes.isEmpty()) {
			    			   wordInceptions.remove(word);
			    		   }
					   }
				   }
			   }
		}
    }
    
    static final Pattern TOKENIZER = Pattern.compile("[^a-zA-Z0-9'\\-]+");
    public static TObjectIntHashMap<String> getWordCounts(Revision rev) {
    	TObjectIntHashMap<String> counts = new TObjectIntHashMap<String>();
    	String cleaned = MarkupStripper.stripEverything(rev.getText());
    	for (String word : TOKENIZER.split(cleaned)) {
    		word = word.toLowerCase();
    		if (!Utils.STOP_WORDS_SET.contains(word)) {
    			counts.adjustOrPutValue(word, 1, 1);
    		}
    	}
    	return counts;
    }
    
    /**
     * Given a dictionary of word counts before and after an edit, return a diff word count dictionary.
     * Positive counts indicate words added, negative counts indicate words removed. 
     * 
     * @param before
     * @param after
     * @return
     */
    public static TObjectIntHashMap<String> getDiff(TObjectIntHashMap<String> before, TObjectIntHashMap<String> after) {
    	Set<String> unionWords = new HashSet<String>();
    	unionWords.addAll(before.keySet());
    	unionWords.addAll(after.keySet());
    	
    	TObjectIntHashMap<String> diff = new TObjectIntHashMap<String>();
    	for (String word : unionWords) {
    		int nBefore = before.containsKey(word) ? before.get(word) : 0; 
    		int nAfter = after.containsKey(word) ? after.get(word) : 0; 
    		if (nBefore != nAfter) {
        		diff.put(word, nAfter - nBefore);
    		}
    	}
    	return diff;
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

        
        job.setJarByClass(PersistentWordRevisions.class);
        //job.setInputFormatClass(AllRevisionsInputFormat.class); 
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
        int res = ToolRunner.run(new PersistentWordRevisions(), args);
        System.exit(res);
    }
}
