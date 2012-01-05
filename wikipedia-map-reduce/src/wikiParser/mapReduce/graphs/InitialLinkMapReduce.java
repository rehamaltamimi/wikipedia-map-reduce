package wikiParser.mapReduce.graphs;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import wmr.core.Page;

import wmr.core.PageParser;
import wmr.core.RevisionFingerprinter;
import wmr.core.Edge;
import wmr.core.EdgeParser;
import wmr.core.Revision;
import wikiParser.mapReduce.util.UniqueConcatenateReduce;
import wikiParser.mapReduce.util.MapReduceUtils;
import wikiParser.mapReduce.util.SimpleJobConf;
import wikiParser.util.LzmaPipe;

public class InitialLinkMapReduce {

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
                EdgeParser lp = new EdgeParser();
                Page article = parser.getArticle();
                RevisionFingerprinter fparser = new RevisionFingerprinter();
                while (true) {
                    Revision rev = parser.getNextRevision();
                    if (rev == null) {
                        break;
                    }
                    for (Edge link : lp.findEdges(fparser, article, rev)) {
                        if (article.isUserTalk() || article.isUser()) {
                            output.collect(new Text("u" + article.getUser().getId()), new Text(link.toOutputString()));
                        } else {
                            output.collect(new Text("a" + article.getId()), new Text(link.toOutputString()));
                        }
                    }
                    fparser.update(rev);
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

    public static void runMe(String inputFiles[], String outputDir, String jobName) throws IOException {
        SimpleJobConf conf = new SimpleJobConf(Map.class, UniqueConcatenateReduce.class, inputFiles, outputDir, jobName);
        conf.run();
    }
}
