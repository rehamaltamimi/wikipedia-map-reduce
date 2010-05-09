package wikiParser.util;


import static org.junit.Assert.assertEquals;


import org.apache.hadoop.io.Text;
import org.junit.Test;
import wikiParser.mapReduce.util.MapReduceUtils;


public class TestMapReduceUtils {
	@Test public void testKeyToLong() throws Exception {
            Text t = new Text("324242.xml.7z");
            assertEquals(MapReduceUtils.keyToId(t), 324242);
	}

}
