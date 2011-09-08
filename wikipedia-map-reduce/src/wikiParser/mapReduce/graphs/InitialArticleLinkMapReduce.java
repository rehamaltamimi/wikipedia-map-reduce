package wikiParser.mapReduce.graphs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import wikiParser.Page;

import wikiParser.PageParser;
import wikiParser.Edge;
import wikiParser.Revision;
import wikiParser.edges.ArticleArticleGenerator;
import wikiParser.mapReduce.util.UniqueConcatenateReduce;
import wikiParser.mapReduce.util.MapReduceUtils;
import wikiParser.mapReduce.util.SimpleJobConf;
import wikiParser.util.LzmaPipe;
/**
 * Creates Article to Article graph with directed edges using links only
 * @author Nathaniel Miller
 */
public class InitialArticleLinkMapReduce {

    /*
     * Takes key-value 7zip hashes and outputs ID-links pairs.
     */
    public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

        public void map(Text key, Text value, OutputCollector<Text, Text> output,
                Reporter reporter) throws IOException {
            
            /*
             * Input: ArticleID-7zipHash key-value pairs.
             * Output: ArticleID-Edge key-value pairs with a 2-digit connection type demarcation.
             * 1. Unzip value
             * 2. Get page info
             * 3. Build connection list
             * 4. Find links
             * 5. Emit individual ID-link pairs with connection type markers.
             */
            LzmaPipe pipe = null;
            try {
                byte[] unescaped = MapReduceUtils.unescape(value.getBytes(), value.getLength());
                pipe = new LzmaPipe(unescaped);
                PageParser parser = new PageParser(pipe.decompress());
                Page article = parser.getArticle();
                ArticleArticleGenerator edgeGenerator = new ArticleArticleGenerator();
                Revision latest = null;
                while (true) {
                    Revision rev = parser.getNextRevision();
                    if (rev == null) {
                        break;
                    }
                    latest = rev;
                }
                if (latest != null) {
                    for (Edge link : edgeGenerator.generateWeighted(article, latest)) {
                        if (article.isUserTalk() || article.isUser()) {
                            output.collect(new Text("u" + article.getUser().getId()), new Text(link.toOutputString()));
                        } else {
                            output.collect(new Text("a" + article.getId()), new Text(link.toOutputString()));
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("error when processing " + key + ":");
                e.printStackTrace();
            } finally {
                if (pipe != null) {
                    pipe.cleanup();
                }
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text,Text,Text,Text> {

        @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            
            HashMap<String,String> edges = new HashMap<String, String>();
            while (values.hasNext()) {
                String v = values.next().toString();
                String k = v.split("\\|")[2];
                if (!edges.containsKey(k)) {
                    edges.put(k,v);
                } else {
                    String[] split = edges.get(k).split("\\|");
                    edges.put(k, split[0] + "|" + Math.max(Integer.parseInt(split[1]), Integer.parseInt(v.split("\\|")[1])) + "|" + k);
                }
            }
            StringBuilder result = new StringBuilder();
            for (String v  : edges.values()) {
                result.append(v).append(" ");
            }
            output.collect(key, new Text(result.toString()));
        }
        
    }
    
    public static void runMe(String inputFiles[], String outputDir, String jobName) throws IOException {
        SimpleJobConf conf = new SimpleJobConf(Map.class, UniqueConcatenateReduce.class, inputFiles, outputDir, jobName);
        conf.run();
    }
}
