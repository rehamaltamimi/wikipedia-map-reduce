
package wikiParser.mapReduce.util;

import gnu.trove.TIntHashSet;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * This reduce relies on hash codes being unique to save memory.
 * This isn't perfectly correct, but it should be okay for our needs.
 * It would be better to introduce a uniquifying map / reduce stage.
 * @author shilad
 */
public class UniqueConcatenateReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output,
            Reporter reporter) throws IOException {
        TIntHashSet added = new TIntHashSet();
        StringBuilder result = new StringBuilder();
        while (values.hasNext()) {
            String v = values.next().toString();
            if (!added.contains(v.hashCode())) {
                result.append(v + " ");
                added.add(v.hashCode());
            }
        }
        output.collect(key, new Text(result.toString()));
    }
}
